class FloodWaveDetector():
    def __init__(self) -> None:
        self.__db_credentials_path = self.read_ini()
        self.dataloader = Dataloader(self.__db_credentials_path)
        self.meta = self.dataloader.meta_data.groupby(["river"]).get_group("Tisza").sort_values(by='river_km',
                                                                                                ascending=False)
        self.gauges = self.meta.dropna(subset=['h_table']).index.tolist()
        self.gauge_peak_plateau_pairs = {}
        self.gauge_pairs = []

        self.path = {}
        self.all_paths = {}
        self.wave_serial_number = 0
        self.branches = LifoQueue()
        self.flood_wave = {}

    @measure_time
    def read_ini(self) -> os.path:
        dirname = os.path.dirname(os.getcwd())
        return os.path.join(dirname, 'database.ini')

    @measure_time
    def mkdirs(self):
        os.makedirs('./saved', exist_ok=True)
        os.makedirs('./saved/step1', exist_ok=True)
        os.makedirs('./saved/step2', exist_ok=True)
        os.makedirs('./saved/step3', exist_ok=True)

    @measure_time
    def peak_plateau_search(self, gauge_ts: np.array) -> np.array:
        """
        Searching for local maximums in a list.
        Currently the function allows to find plateaus.

        :param np.array gauge_ts: Time series we search the local maximim values in.
        :return np.array: array of boolean values - True if there is a local max, false if not.
        """

        # Shift and crop the time series to align the trio of values in the time series
        past = gauge_ts[0:-2]
        present = gauge_ts[1:-1]
        future = gauge_ts[2:]

        # Boolean index array for non-decreasing trend continues with non-increasing trend
        non_decreasing = (present - past) >= 0
        non_increasing = (future - present) <= 0
        peak_plateau = non_decreasing & non_increasing

        return np.append(np.append([False], peak_plateau), [False])

    @measure_time
    def create_peak_plateau_list(
            self,
            gauge_df: pd.DataFrame,
            bool_filter: np.array,
            reg_number: str
    ) -> list:
        """
        Returns with the list of found (date, peak/plateau value) tuples for a single gauge

        :param pd.DataFrame gauge_df: One gauge column, one date column, date index.
        :param np.array bool_filter: A bool list. There is a local peak/plateau, where it's true.
        :param str reg_number: The gauge id.
        :return list: list of tuple of local max values and the date. (date, value)
        """

        # Clean-up dataframe for getting peak-plateau list
        peak_plateau_df = gauge_df.loc[bool_filter]
        peak_plateau_df["date"] = peak_plateau_df.index
        peak_plateau_df.index = peak_plateau_df["date"].dt.strftime('%Y-%m-%d')
        peak_plateau_df = peak_plateau_df.drop(columns=["date", "Date"])
        peak_plateau_df[reg_number] = peak_plateau_df[reg_number].astype(float)

        # Get peak-plateau list
        peak_plateau_tuple = peak_plateau_df.to_records(index=True)
        peak_plateau_list = [tuple(x) for x in peak_plateau_tuple]
        return peak_plateau_list

    def filter_for_start_and_length(
            self,
            gauge_df: pd.DataFrame,
            min_date: datetime,
            window_size: int
    ) -> pd.DataFrame:
        """
        Find possible follow-up dates for the flood wave coming from the previous gauge

        :param pd.DataFrame gauge_df: Dataframe to crop
        :param datetime min_date: start date of the crop
        :param int window_size: size of the new dataframe (number of days we want)
        :return pd.DataFrame: Cropped dataframe with found next dates.
        """

        max_date = min_date + timedelta(days=window_size)
        found_next_dates = gauge_df[(gauge_df['Date'] >= min_date) & (gauge_df['Date'] <= max_date)]

        return found_next_dates

    @measure_time
    def search_flooding_gauge_pairs(self, delay: int, window_size: int, gauges: list) -> None:
        """
        Creates the wave-pairs for gauges next to each other.
        Creates separate jsons and a actual_next_pair (super_dict) including all the pairs with all of their waves.

        :param int delay: Minimum delay (days) between two gauges.
        :param int window_size: Size of the interval (days) we allow a delay.
        :param list gauges: The id list of the gauges (in order).
        """

        gauge_peak_plateau_pairs = {}
        big_json_exists = os.path.exists('./saved/step2/gauge_peak_plateau_pairs.json')

        for actual_gauge, next_gauge in itertools.zip_longest(gauges[:-1], gauges[1:]):
            actual_json_exists = os.path.exists(f'saved/step2/{actual_gauge}_{next_gauge}.json')

            if (not actual_json_exists) or (not big_json_exists):

                # Read the data from the actual gauge.
                actual_gauge_f = open(f'./saved/step1/{actual_gauge}.json')
                actual_gauge_with_index = json.load(actual_gauge_f)
                actual_gauge_f.close()
                actual_gauge_df = pd.DataFrame(data=actual_gauge_with_index,
                                               columns=['Date', 'Max value'])
                actual_gauge_df['Date'] = pd.to_datetime(actual_gauge_df['Date'])

                # Read the data from the next gauge.
                next_gauge_f = open(f'./saved/step1/{next_gauge}.json')
                next_gauge_with_index = json.load(next_gauge_f)
                next_gauge_f.close()
                next_gauge_df = pd.DataFrame(data=next_gauge_with_index,
                                             columns=['Date', 'Max value'])
                next_gauge_df['Date'] = pd.to_datetime(next_gauge_df['Date'])

                # Create actual_next_pair
                actual_next_pair = dict()
                for actual_date in actual_gauge_df['Date']:

                    # Find next dates for the following gauge
                    past_date = actual_date - timedelta(days=delay)
                    found_next_dates = self.filter_for_start_and_length(
                        gauge_df=next_gauge_df,
                        min_date=past_date,
                        window_size=window_size
                    )

                    # Convert datetime to string
                    found_next_dates_str = []
                    if not found_next_dates.empty:
                        for found_date in found_next_dates['Date']:
                            found_date = found_date.strftime('%Y-%m-%d')
                            found_next_dates_str.append(found_date)
                        actual_next_pair[actual_date.strftime('%Y-%m-%d')] = found_next_dates_str

                # Save to file
                file = open(f'./saved/step2/{actual_gauge}_{next_gauge}.json', 'w')
                json.dump(obj=actual_next_pair,
                          fp=file,
                          indent=4)
                file.close()

                # Store result for the all in one dict
                gauge_peak_plateau_pairs[f'{actual_gauge}_{next_gauge}'] = actual_next_pair
                print(f'{actual_gauge}_{next_gauge}.json')

        # Save to file
        if not gauge_peak_plateau_pairs == {}:
            file_all = open('./saved/step2/gauge_peak_plateau_pairs.json', 'w')
            json.dump(obj=gauge_peak_plateau_pairs,
                      fp=file_all,
                      indent=4)
            file_all.close()

    def create_flood_wave(self, next_gauge_date: str,
                          next_idx: int) -> None:
        """
        Recursive function walking along the paths in the rooted tree representing the flood wave
        We assume that global variable path contains the complete path up to the current state
        i.e. all nodes (=gauges) are stored before the call of create_flood_wave

        :param str next_gauge_date: The next date, we want to find in the next pair's json.
        A date from the list, not the key. Date after the branch
        :param int next_idx: Index of the next gauge pair.
        E.g: index 1 is referring to "1515-1516" if the root is "1514-1515".
        """

        # other variables
        max_index_value = len(self.gauge_peak_plateau_pairs.keys()) - 1
        next_gauge_pair = self.gauge_pairs[next_idx]
        next_gauge = next_gauge_pair.split('_')[1]
        next_gauge_pair_date_dict = self.gauge_peak_plateau_pairs[next_gauge_pair]

        # See if we continue the wave
        can_path_be_continued = next_gauge_date in next_gauge_pair_date_dict.keys()
        if can_path_be_continued and next_idx < max_index_value:

            # Get new data values
            new_date_value = next_gauge_pair_date_dict[next_gauge_date]
            # the recursion continues with the first date
            new_gauge_date = new_date_value[0]

            # we store the other possible dates for continuation in a LiFoQueue
            if len(new_date_value) > 1:

                # Save the informations about the branches in a LiFoQueue (branches) so we can come back later.
                for k, dat in enumerate(new_date_value[1:]):
                    path_partial = deepcopy(self.path)  # copy result up to now
                    path_partial[next_gauge] = dat  # update with the new node and the corresponding possible date
                    new_path_key = "path" + str(next_idx + 1) + str(k)
                    self.all_paths[new_path_key] = path_partial
                    self.branches.put([dat, next_idx + 1, new_path_key])

            # Update the status of our "place" (path)
            self.path[next_gauge] = new_gauge_date

            # Keep going, search for the path
            self.create_flood_wave(next_gauge_date=new_gauge_date,
                                   next_idx=next_idx + 1)
        else:

            # Update the 'map'. (Add the path to the start date)
            self.flood_wave['id' + str(self.wave_serial_number)] = self.path

            # Make possible to have more paths
            self.wave_serial_number += 1

    @measure_time
    def sort_wave(self, filenames: list,
                  start: str = '2006-02-01',
                  end: str = '2006-06-01') -> list:
        """
        It's hard to visualize waves far from each other.
        With this method, we can choose a period and check the waves in it.

        :param list filenames: List of filenames we want to choose from. (Usually all files from the directory)
        :param str start: Start date of the interval.
        :param str end: Final day of the interval.
        :return str filename_sort: List of filenames with waves in the given interval.
        """
        start = datetime.strptime(start, '%Y-%m-%d')
        end = datetime.strptime(end, '%Y-%m-%d')

        filename_sort = []

        for fl in filenames:
            date_str = fl.split(".json")[0]
            date_dt = datetime.strptime(date_str, '%Y-%m-%d')

            if date_dt >= start and date_dt <= end:
                filename_sort.append(fl)

        return filename_sort

    @measure_time
    def plot_graph(self):
        filenames = next(os.walk('./saved/step3'), (None, None, []))[2]  # [] if no file

        fig = plt.figure(figsize=(15, 6))

        # Add title
        title = 'Flood wave'
        plt.title(title)

        # Select waves from a time interval
        sorted_filenames = self.sort_wave(filenames=filenames, start='2005-10-01', end='2006-01-01')

        max_array = []

        # Read all the files:
        for file in sorted_filenames:

            # Initialize
            flood_dict = {}

            # Read a file and load it
            file_path = './saved/step3/' + file
            file_opened = open(file_path)
            flood_dict = json.load(file_opened)
            file_opened.close()

            # Get keys
            keys = flood_dict.keys()
            key_list = list(keys)

            # Read every dict of the file
            for key_o in key_list:
                local_dict = flood_dict[key_o]
                new_list = []
                # Create an array from the values to plot
                for key, value in iter(local_dict.items()):
                    new_list.append([key, datetime.strptime(value, '%Y-%m-%d')])

                if len(max_array) < len(new_list):
                    max_array = new_list
                my_array = np.array([np.array(xi) for xi in new_list])

                # Get x and y
                y = my_array[:, 0]
                x = my_array[:, 1]

                # Plot
                for i in range(0, len(x), 1):
                    plot = plt.plot(x[i:i + 2], y[i:i + 2], 'bo-')

                max_array = np.array([np.array(xi) for xi in max_array])

        # Plot the longest wave again, so the image isn't cropped.
        # Get x and y
        y = max_array[:, 0]
        x = max_array[:, 1]

        # Plot
        for i in range(0, len(x), 1):
            plot = plt.plot(x[i:i + 2], y[i:i + 2], 'bo-')

        # plt.xticks(rotation = 45)
        plt.ylim(y[-1], y[0])
        plt.show()

    @measure_time
    def step_1(self):
        for gauge in self.gauges:
            if not os.path.exists('saved/step1/' + str(gauge) + '.json'):
                # Get gauge data and drop missing data and make it an array.
                gauge_df = self.dataloader.get_daily_time_series(reg_number_list=[gauge]).dropna()

                # Get local peak/plateau values
                peak_plateau_bool = self.peak_plateau_search(gauge_ts=gauge_df[str(gauge)].to_numpy())

                # Create keys for dictionary
                peak_plateau_tuples = self.create_peak_plateau_list(
                    gauge_df=gauge_df,
                    bool_filter=peak_plateau_bool,
                    reg_number=str(gauge)
                )

                # Save
                file = open('./saved/step1/' + str(gauge) + ".json", "w")
                json.dump(obj=peak_plateau_tuples,
                          fp=file,
                          indent=4)
                file.close()

                print(str(gauge) + ".json")

    @measure_time
    def step_2(self):
        self.search_flooding_gauge_pairs(delay=0, window_size=3, gauges=self.gauges)

    @measure_time
    def step_3(self):
        """
        Searching for wave "series". For now, starting from the root ('1514-1515').
        Trying to find the same waves in different gauges.
        """

        # Read the gauge_peak_plateau_pairs (super dict)
        if self.gauge_peak_plateau_pairs == {}:
            gauge_peak_plateau_pairs_f = open('./saved/step2/gauge_peak_plateau_pairs.json')
            self.gauge_peak_plateau_pairs = json.load(gauge_peak_plateau_pairs_f)
            gauge_peak_plateau_pairs_f.close()

        print(self.gauge_peak_plateau_pairs)

        print(list(self.gauge_peak_plateau_pairs.keys()))
        self.gauge_pairs = list(self.gauge_peak_plateau_pairs.keys())

        next_g_p_idx = 1
        """
        To understand the code better, here are some description and example:

        branches = ['1951-01-11', 3, 'path30']

            We save the branches to this stucture. It' a . 
            This means, we can get out first the element , we put in last. 
            On the above example we can see one element of branches.


        path =  {'1514': '1951-01-07', '1515': '1951-01-08', '1516': '1951-01-08', '1518': '1951-01-09'}

            One path without branches.


        all_paths ={'path20': {'1514': '1951-01-07', '1515': '1951-01-08', '1516': '1951-01-08', '1518': '1951-01-09', 
                               '1520': '1951-01-09', '1521': '1951-01-09', '1719': '1951-01-10'}, 
                    'path30': {'1514': '1951-01-07', '1515': '1951-01-08', '1516': '1951-01-08', '1518': '1951-01-09'}}

            More path from the same start. The last path might be unfinished.


        flood_wave = {'id0': {'1514': '1951-01-07', '1515': '1951-01-08', '1516': '1951-01-08', '1518': '1951-01-08', 
                              '1520': '1951-01-09', '1521': '1951-01-09', '1719': '1951-01-10'}, 
                      'id1': {'1514': '1951-01-07', '1515': '1951-01-08', '1516': '1951-01-08', '1518': '1951-01-08'}, 
                      'id2': {'1514': '1951-01-07', '1515': '1951-01-08', '1516': '1951-01-08', '1518': '1951-01-09', 
                              '1520': '1951-01-09', '1521': '1951-01-09', '1719': '1951-01-10'}, 
                      'id3': {'1514': '1951-01-07', '1515': '1951-01-08', '1516': '1951-01-08', '1518': '1951-01-09'}}

            All of the waves from the given start point. 
        """

        root_gauge_pair = self.gauge_pairs[0]  # Root.
        root_gauge_pair_date_dict = self.gauge_peak_plateau_pairs[root_gauge_pair]

        # Search waves starting from the root
        for actual_date in root_gauge_pair_date_dict.keys():

            print("ACTUAL_DATE", actual_date)
            print('#####################################################')

            self.flood_wave = {}

            # Go over every date with a wave
            for next_date in root_gauge_pair_date_dict[actual_date]:

                # Empty and reset variables
                next_g_p_idx = 1
                self.path = {}
                self.all_paths = {}
                self.wave_serial_number = 0

                root_gauge = root_gauge_pair.split('_')[0]
                root_gauge_next = root_gauge_pair.split('_')[1]

                self.path[root_gauge] = actual_date
                self.path[root_gauge_next] = next_date

                # Search for flood wave
                self.create_flood_wave(next_gauge_date=next_date,
                                       next_idx=next_g_p_idx)

                # Go over the missed branches
                while self.branches.qsize() != 0:
                    # Get info from branches (info about the branch)
                    new_date, new_g_p_idx, path_key = self.branches.get()
                    self.path = self.all_paths[path_key]

                    # Go back to the branch
                    self.create_flood_wave(next_gauge_date=new_date,
                                           next_idx=new_g_p_idx)

                # Save the wave
                file = open(f'./saved/step3/{actual_date}', 'w')
                json.dump(obj=self.flood_wave,
                          fp=file,
                          indent=4)
                file.close()

    @measure_time
    def run(self):
        self.mkdirs()
        self.step_1()
        self.step_2()
        self.step_3()