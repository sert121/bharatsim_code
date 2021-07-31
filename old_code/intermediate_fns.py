# Important Variables
# STATE = "Maharashtra"
# DISTRICTS = {"Pune": 3132143}
#DISTRICTS = {"Bellary": 2452595}


import time
from ctgan import CTGANSynthesizer
import pandas as pd
from math import ceil
import places
from opencage.geocoder import OpenCageGeocode
import numpy as np
import gc

  # Adding adherence to lockdown
def f(row):
    if (35 <= row['Age'] <=  40):
        val = 0.0

    elif (30 <= row['Age'] <  35):
        val = 0.1

    elif (25 <= row['Age'] <  30):
        val = 0.2

    elif (20 <= row['Age'] <  25):
        val = 0.3

    elif (15 <= row['Age'] <  20):
        val = 0.4

    elif (10 <= row['Age'] <  15):
        val = 0.8

    elif (0 <= row['Age'] <  10):
        val = 1.0
    
    elif (60 <= row['Age'] <  100):
        val = 1.0
    
    elif (40 <= row['Age'] <  60):
        val = 0.9

    else:
        val = 1.0
    return val

def add_jobs_pub_transport_adherence(STATE, DISTRICTS, samples):

    gc.collect()


    # Add district name in the generated data
    samples['District'] = district

    population_counts = DISTRICTS

    print("ADDING JOB COLUMNS...")
    # Generate job columns
    job_generator = CTGANSynthesizer()
    job_generator = job_generator.load("job.pkl")
    jobs = job_generator.sample(population_counts)

    # Split Job into JobLabel and JobID
    job_label_id = pd.DataFrame(jobs.Job.str.rsplit(' ',1).tolist(), columns = ['JobLabel','JobID'])
    samples = pd.concat([samples, job_label_id], axis=1)
    gc.collect()

    #samples['Job']

    # Essential worker columns list
    print("Adding essential workers...")
    essential_list = ['Police', 'Sweepers', 'Sales, shop', 'Shopkeepers', 'Boilermen', 'Nursing', 'Journalists', 'Electrical', 'Food', 'Physicians', 'Mail distributors', 'Loaders', 'Village officials', 'Govt officials', 'Telephone op']
    samples['essential_worker'] = np.where(samples['JobLabel'].isin(essential_list), 1, 0)
    samples.loc[samples['Age'] <=16, ['essential_worker', 'JobLabel', 'JobID']] = 0, "Student", 150

    samples.loc[samples['JobID'] == 'X1', 'JobID'] = 151
    samples.loc[samples['JobID'] == 'EE', 'JobID'] = 152
    samples.loc[samples['JobID'] == 'X9', 'JobID'] = 153
    samples.loc[samples['JobID'] == 'AA', 'JobID'] = 154


    # Public Transport
    print("Adding public transport...")
    unique_jobs = samples.JobLabel.unique()
    transport_dict = {key:1 for key in unique_jobs}
    privTransport_Jobs = ['Police','Govt Officials','Teachers','Engineers','Managerial nec','Production nec','Textile','Professional nec','Journalists','Economists','Jewellery','Shopkeepers','Physicians','Computing op','Mgr manf','Technical sales']

    for job in transport_dict.keys():
        if job in privTransport_Jobs:
            transport_dict[job]=0

    samples['PublicTransport_Jobs'] = np.nan
    for index, row in samples.iterrows():
        val = transport_dict[row['JobLabel']]
        samples.at[index,'PublicTransport_Jobs'] = val 


  

    samples['Adherence_to_Intervention'] = samples.apply(f, axis=1)
    samples = samples.astype({'WorkPlaceID': 'int64', 'school_id': 'int64', 'PublicTransport_Jobs': 'int64', 'Age' : 'int64', 'essential_worker' : 'int64'})
    gc.collect()

    # Save the file to csv
    print("Saving file..")
    samples.to_csv("{}/Synthetic/{}_{}.csv".format(STATE, STATE, district), index=False)
    gc.collect()
