# boston_property_data_fetcher.py

import urllib.request
import xmltodict
import pandas as pd

class BostonPropertyDataFetcher:
    def __init__(self, url):
        self.url = url

    def fetch_data(self):
        rows = []

        i = 0
        while self.url:
            i = i + 1
            print("Loop -", i)
            fileobj = urllib.request.urlopen(self.url)

            response_xml = fileobj.read().decode('utf-8')

            data_dict = xmltodict.parse(response_xml)

            entries = data_dict['feed']['entry']

            for entry in entries:
                properties = entry['content']['m:properties']
                row = {key: val.get('#text', None) for key, val in properties.items()}
                rows.append(row)

            self.url = None

            links = data_dict['feed']['link']
            if isinstance(links, list):
                for link in links:
                    if link['@rel'] == 'next':
                        self.url = link['@href']
                        break
            elif isinstance(links, dict) and links['@rel'] == 'next':
                self.url = links['@href']

        df = pd.DataFrame(rows)

        df.to_csv('BostonProp.csv', index=False)

        assessment_data = pd.read_csv('BostonProp.csv')
        assessment_data.columns = assessment_data.columns.str[2:]

        return assessment_data
    
class DataProcessor:
    def process_data(self, df):
        duplicates_in_data = df[df.duplicated(['PID'])]
        print(f"The duplicates found in the PID column: \n {duplicates_in_data.count()} \n")

        df.drop_duplicates(subset='PID', keep='last', inplace=True)

        duplicates_in_data = df[df.duplicated(['PID'])]
        print(f"The duplicates found in the PID column: \n {duplicates_in_data.count()}")
        
        #Drop first _id column
        df.drop(columns='_id', inplace=True)

        return df    

    def describe_numerical_columns(self, df):
        return df.describe(datetime_is_numeric=True, include="all")

    def describe(self, df):
        return df.describe()
    