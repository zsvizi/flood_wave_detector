import os
PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import sys
sys.path.append(PROJECT_PATH)
print(sys.path)
from src.flood_wave_detector import FloodWaveDetector
