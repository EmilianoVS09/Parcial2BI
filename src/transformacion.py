import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
import ast
from collections import Counter
import re
import unicodedata
import numpy as np
from pathlib import Path
from extraction import Extraction
from logs import Logs

uri = "mongodb://localhost:27017/"
db_name = "bi_mx"
extra = Extraction() #Creación del objeto de la clase Extraction


class Transformation:
    #Constructor con la clase logs
    def __init__(self):
        self.logs = Logs()

    #