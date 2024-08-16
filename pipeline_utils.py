import pandas as pd
import os

def read_data(filename: str):
    # Read csv into df, update column names
    new_column=['id','sis_id','section','section_id','section_sis_id','submit_date','attempt','1','2',
                'job_type', '3','job_status','4','organization','5','work_zipcode','6', 'age','7',
                'race','8','Hipsanic/Latino','9','year_of_experience', '10', 'population_group','11',
                'gender','12','n_correct','n_incorrect','score']
    output_df = pd.read_csv(filename, header=0, names=new_column)
    return output_df

def drop_column(input_df: pd.DataFrame):
    # drop useless columns
    column_list = ['1','2','3','4','5','6','7','8','9','10','11','12','sis_id','section_sis_id','section','section_id',
                    'n_correct','n_incorrect','score']
    output_df = input_df.drop(column_list, axis=1)
    return output_df

def normalize_datetime(df: pd.DataFrame):
    # update 'submit_date' colume to datetime type, normalize to date only
    df['submit_date'] = pd.to_datetime(df['submit_date'])
    df['submit_date'] = df['submit_date'].dt.normalize()
    return df

def drop_duplicates(input_df: pd.DataFrame):
    # Drop duplicates by only keeping 1 attempt columns
    output_df = input_df[input_df['attempt']==1]
    return output_df

def drop_na_row(df: pd.DataFrame, threshold):
    # Drop rows that have 3 or more missing values
    subset_col=['job_type','job_status','organization','work_zipcode','age','race','Hipsanic/Latino',
            'year_of_experience','population_group','gender']
    df.dropna(subset=subset_col, thresh=threshold, inplace=True)
    return df

def get_all_csv_files(directory):
    # Get all csv files within the specified directory
    csv_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
    return csv_files

def process_files(input_directory, output_directory):
    # Process all csv files in the input directory and save the processed files to the output directory
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    csv_files = get_all_csv_files(input_directory)
    
    for fname in csv_files:
        df = read_data(fname)
        df = drop_column(df)
        df = normalize_datetime(df)
        df = drop_duplicates(df)
        df = drop_na_row(df, 7)
        
        output_fname = os.path.join(output_directory, os.path.basename(fname).replace('.csv', '_processed.csv'))
        df.to_csv(output_fname, index=False)