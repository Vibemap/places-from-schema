# %%
import pandas as pd
import glob

import pprint
pp = pprint.PrettyPrinter(indent=4)


# %%
print('Import select files from the Common Crawl project and join them into a single Pandas Databframe for analysis.')

# %%
path = r'/workspaces/places-from-schema/data'  # use your path
all_files = glob.glob(path + "/*.json.gz")

print(all_files)
# %%
# Join all the files into one dataframe.
file_list = []
for filename in all_files:
    df = pd.read_json(
      filename,
      lines=True,
      compression='gzip'
    )
    file_list.append(df)

# %%
combined_frame = pd.concat(file_list, axis=0, ignore_index=True)

# %%
combined_frame.count()
# %%
combined_frame.head(20)
# %%
#combined_frame = combined_frame.append(pd.json_normalize(combined_frame.address))
# %%
# Next make a dictionary and select fields to include
columns = [
  'acceptsreservations',
  'address',
  'aggregaterating',
  'category',
  'description',
  'geo',
  'image',
  'name',
  'openinghours',
  'page_url',
  'pricerange',
  'rating',
  'review',
  'servescuisine',
  'telephone',
  'url'
]

df_to_dict = combined_frame[columns].to_dict(orient='records')
# %%
print('This many places ', len(df_to_dict))
pp.pprint(df_to_dict[0])

# %%
flat_dict = []
for item in df_to_dict:
  #print(item['aggregaterating'], type(item['aggregaterating']) is dict)
  if item['aggregaterating'] and type(item['aggregaterating']) is dict:
    item['rating'] = item['aggregaterating']['ratingvalue'].replace('E0', '')

    if 'reviewcount' in item['aggregaterating']:
      item['review_count'] = float(item['aggregaterating']['reviewcount'])

  if item['address'] and type(item['address']) is dict and 'addresslocality' in item['address']:
    item['city'] = item['address']['addresslocality']

  if item['geo'] and type(item['geo']) is dict:
    try:
      item['latitude'] = item['geo']['latitude']
      item['longitude'] = item['geo']['longitude']

  flat_dict.append(item)

new_df = pd.DataFrame.from_dict(flat_dict)
new_df_sorted = new_df.sort_values(by=['reviewcount'], ascending=False)
# %%
new_df.city.head(1000)
# %%
# Get a unique dict of all cities
unique_cities = new_df.city.unique()
# %%
for x in unique_cities:
  print(x)
# %%
df_with_cities = new_df[new_df['city'].notnull()]
df_with_cities.head()

# %%
los_angeles = df_with_cities[df_with_cities['city'].str.contains("Los Angeles")]

los_angeles_sorted = los_angeles.sort_values(by=['review_count'], ascending=False)

los_angeles_sorted.count()

export_columns = ['name', 'review_count', 'rating', 'longitude', 'latitude']
los_angeles_sorted.to_csv('./data/los_angeles_sorted.csv')

# %%
fort_lauderdale = df_with_cities[df_with_cities['city'].str.contains("Fort Lauderdale")]
fort_lauderdale.count()
# %%