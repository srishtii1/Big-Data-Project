# import seaborn as sns
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


FILE_PATH = './data/data.csv'

def plot(df, key):
    stats = dict(df[key].value_counts())

    stats = dict(sorted(stats.items()))

    keys = [str(x) for x in list(stats.keys())]
    values = list(stats.values())
      
    fig = plt.figure(figsize = (10, 5))
    
    # creating the bar plot
    plt.bar(keys, values, color ='maroon',
            width = 0.4)
    
    plt.ylabel("Num Occurences")
    plt.title(f"Distirbution of {key}")

    if len(keys) > 5:
      plt.xticks(rotation=45)

    plt.ylim([0, 476920])
    plt.show()


df = pd.read_csv(FILE_PATH)
plot(df, 'City')
plot(df, 'Year')