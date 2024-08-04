import pandas as pd
import matplotlib.pyplot as plt
import glob

# Read all CSV files in the output directory
files = glob.glob("output/*.csv")
df = pd.concat([pd.read_csv(f) for f in files])

# Plot the data
plt.figure(figsize=(10, 6))
df.groupby('word')['count'].sum().sort_values(ascending=False).plot(kind='bar')
plt.title('Hashtag Popularity')
plt.xlabel('Hashtags')
plt.ylabel('Count')
plt.show()