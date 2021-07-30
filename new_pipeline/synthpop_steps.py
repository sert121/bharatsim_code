from synthpop.synthpop.zone_synthesizer import synthesize_all_zones, load_data #From https://github.com/UDST/synthpop.git
import pandas as pd
import numpy as np
import random
from io import StringIO
import geopandas as gpd
from shapely.geometry import Point, MultiPoint
from shapely.ops import unary_union

from tqdm import tqdm
tqdm.pandas()

###Helper functions for data cleaning
def try_convert(value, default, *types):
	for t in types:
		try:
			return t(value)
		except (ValueError, TypeError):
			continue
	return default

class IPU():
	def __init__(self):
		pass

	def preprocess_individual_samples_ihds(self, filtered_ihds_individuals_data):

		columns_to_keep_individuals = ['DISTRICT', 'IDHH', 'PERSONID', 'RO3', 'RO6', 'RO5','ED2', 'ID11', 'ID13', 'RO7', 'URBAN2011']
		columns_rename_dict_individuals = {'RO3':'gender', 'RO5':'age', 'RO6':'marital_status',
			'ED2':'literacy', 'ED6':'edu_years', 'EDUC7': 'edu_cat',
			'ID11':'religion', 'ID13':'caste', 'URBAN2011':'residence',
			'WS4':'job', 'RO7':'activity_status', 'IDHH':'serialno', 'PERSONID':'mem_id',
			'DIST01':'district', 'MB3':'M_Cataract', 'MB4':'M_TB', 'MB5':'M_High_BP',
			'MB6':'M_Heart_disease', 'MB7':'M_Diabetes', 'MB8':'M_Leprosy',
			'MB9':'M_Cancer', 'MB10':'M_Asthma', 'MB11':'M_Polio',
			'MB12':'M_Paralysis', 'MB13':'M_Epilepsy', 'SM4':'M_Fever', 'SM5':'M_Cough',
			'SM7':'M_Diarrhea'}

		filtered_ihds_individuals_data = filtered_ihds_individuals_data[columns_to_keep_individuals]
		filtered_ihds_individuals_data = filtered_ihds_individuals_data.rename(columns_rename_dict_individuals, axis='columns')

		individuals_data = filtered_ihds_individuals_data.dropna()

		gender_dict = {1:'male', '1':'male', 2:'female', '2':'female'}
		individuals_data['gender'] = individuals_data['gender'].map(gender_dict)

		individuals_data.loc[individuals_data['marital_status']==' ','marital_status'] = 1
		individuals_data['marital_status'] = individuals_data['marital_status'].astype(int)
		marital_dict = {0:'married', 1:'married', 2:'unmarried', 3:'widowed', 4: 'separated', 5: 'married'}
		individuals_data['marital_status'] = individuals_data['marital_status'].map(marital_dict)

		individuals_data.loc[individuals_data['literacy']==' ','literacy'] = 0
		# individuals_data['activity_status'] = individuals_data['activity_status'].astype(int)
		# individuals_data.loc[(individuals_data['literacy']==' ') & (individuals_data['activity_status'] >=5) & (individuals_data['activity_status']<=10),'literacy']=1
		# individuals_data.loc[(individuals_data['literacy']==' ') & (individuals_data['activity_status'] ==12) & (individuals_data['age']!='0to4'),'literacy']=1

		individuals_data['literacy'] = individuals_data['literacy'].astype(int)
		individuals_data.loc[individuals_data.literacy == 1, 'literacy'] = 'literate'
		individuals_data.loc[individuals_data.literacy == 0, 'literacy'] = 'illiterate'

		bins= [0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,110]
		labels = ['0to4', '5to9', '10to14', '15to19','20to24', '25to29','30to34', '35to39', '40to44', '45to49',
				 '50to54', '55to59', '60to64', '65to69', '70to74', '75to79', '80p']

		individuals_data['age'] = individuals_data['age'].apply(lambda x : try_convert(x, np.float('nan'), int) )   

		individuals_data['age'] = pd.cut(individuals_data['age'], bins=bins, labels=labels, right=False)

		religion_dict = {1: 'hindu', 2:'muslim', 3:'christian', 4:'sikh', 5:'buddhist', 6:'jain',
						7: 'other', 8:'other', 9:'other'}
		individuals_data['religion'] = individuals_data['religion'].map(religion_dict)

		individuals_data.loc[individuals_data['caste']==' ', 'caste'] =  random.randint(1,6)
		individuals_data.loc[(individuals_data['caste']==' ') & (individuals_data['religion']!='hindu'),'caste'] = 6
		individuals_data['caste'] = individuals_data['caste'].astype(int)
		caste_dict = {4: 'SC', 5:'ST', 1:'other', 2:'other', 3:'other', 6:'other'}
		individuals_data['caste'] = individuals_data['caste'].map(caste_dict)

		urbandict = {1:'urban', 0:'rural'}
		individuals_data['residence'] = individuals_data['residence'].map(urbandict)

		individuals_data['working'] = 'yes'
		individuals_data['activity_status'] = individuals_data['activity_status'].apply(lambda x : try_convert(x, np.float('nan'), int) )   
		individuals_data.loc[individuals_data.activity_status >= 10, 'working'] = 'no'

		individuals_data = individuals_data.drop(['job', 'activity_status', 'edu_years'], axis=1, errors='ignore')

		individuals_data.loc[individuals_data['literacy']=='illiterate','edu_cat'] = 'illiterate'
		individuals_data['edu_cat'] = individuals_data['edu_cat'].astype(str)
		individuals_data.loc[individuals_data['edu_cat']=='0','edu_cat'] = 'illiterate'
		edu_dict = {'3': 'below_primary', '5':'primary', '8':'middle', '10':'secondary', '12':'senior_secondary',
				   '15':'grad_p', '16':'grad_p'}
		individuals_data['edu_cat'].replace(edu_dict, inplace=True)

		return individuals_data

	def preprocess_household_samples_ihds(self, filtered_ihds_households_data):

		columns_to_keep_households = ['DIST01', 'IDHH', 'URBAN2011', 'NPERSONS']
		columns_rename_dict_households = {'URBAN2011':'residence', 'IDHH':'serialno','DIST01':'district', 'NPERSONS':'hhsize'}

		households_data = filtered_ihds_households_data[columns_to_keep_households]
		households_data = households_data.rename(columns_rename_dict_households, axis='columns')

		urbandict = {1:'urban', 0:'rural'}
		households_data['residence'] = households_data['residence'].map(urbandict)

		hhsize_dict = {1:'hhsize_1', 2:'hhsize_2', 3:'hhsize_3', 4:'hhsize_4', 5:'hhsize_5',
					  6:'hhsize_6', 7:'hhsize_710', 8:'hhsize_710', 9:'hhsize_710',
					  10:'hhsize_710', 11:'hhsize_1114', 12:'hhsize_1114', 13:'hhsize_1114',
					  14:'hhsize_1114'}

		households_data.loc[households_data['hhsize'] >=15, 'hhsize'] = 'hhsize_15p'
		households_data['hhsize'] = households_data['hhsize'].replace(hhsize_dict)
		return households_data

	def load_marginals(self, householdh_marginal_filename, individuals_marginal_filename, individuals_data, households_data):
		empty_file_households = StringIO("1,2,3") #Creating empty files so that load_data function can be used which is built to load samples as well
		empty_file_individuals = StringIO("1,2,3") #Creating empty files so that load_data function can be used which is built to load samples as well

		household_marginal, individuals_marginal, hh_sample_empty, p_sample_empty, xwalk = load_data(householdh_marginal_filename, individuals_marginal_filename, empty_file_households, empty_file_individuals)

		household_marginal = household_marginal[list(household_marginal.columns)].astype(float)
		household_marginal = household_marginal[list(household_marginal.columns)].astype(int)

		individuals_marginal = individuals_marginal[list(individuals_marginal.columns)].astype(float)
		individuals_marginal = individuals_marginal[list(individuals_marginal.columns)].astype(int)

		district_dict = pd.Series(individuals_marginal.index, index=individuals_marginal.distid.distid.values).to_dict()
		individuals_data['DISTRICT'] = individuals_data['DISTRICT'].replace(district_dict)
		households_data['district'] = households_data['district'].replace(district_dict)
		households_data['sample_geog'] = 1
		individuals_data['sample_geog'] = 1

		household_marginal.drop('distid', axis=1, inplace=True)

		individuals_marginal = individuals_marginal.drop(['distid','total_pop', 'residence'], axis=1)
		individuals_marginal = individuals_marginal.drop(['illiterate_males','illiterate_females', 
							 'literate_males', 'literate_females',
							 'marginal_less3', 'marginal_6', 'non_worker'], axis=1, level=1)
		individuals_marginal = individuals_marginal.rename({'main_workers': 'yes', 'non_worker2': 'no'}, axis='columns', level=1)

		individuals_marginal[('marital_status','separated')] = (individuals_marginal['marital_status']['separated'] + individuals_marginal['marital_status']['divorced']).values

		individuals_marginal[('edu_cat','senior_secondary')] = (individuals_marginal['edu_cat']['senior_secondary'] + individuals_marginal['edu_cat']['dip_cert_nontech'] + individuals_marginal['edu_cat']['dip_cert_tech']).values
		individuals_marginal[('edu_cat','illiterate')] = (individuals_marginal['edu_cat']['illiterate'] + individuals_marginal['edu_cat']['lit_wo_edu']).values

		individuals_marginal.drop(['divorced','dip_cert_nontech', 'dip_cert_tech', 'lit_wo_edu'], axis=1, level=1, inplace=True)

		individuals_marginal = individuals_marginal.drop(['marital_status', 'edu_cat'], axis=1)

		district_not_in_survey = [] ####### remove rows based on data. This step needs to be adjusted when we add many rows to the marginal file.
		xwalk_dict = dict(xwalk)
		xwalk_dict = {key: xwalk_dict[key] for key in xwalk_dict if key not in district_not_in_survey}
		xwalk = list(tuple(xwalk_dict.items()))

		return household_marginal, individuals_marginal, xwalk

	def generate_data(self, filtered_ihds_individuals_data, filtered_ihds_households_data, householdh_marginal_filename, individuals_marginal_filename):
		individuals_data = self.preprocess_individual_samples_ihds(filtered_ihds_individuals_data)

		households_data = self.preprocess_household_samples_ihds(filtered_ihds_households_data)

		household_marginal, individuals_marginal, xwalk = self.load_marginals(householdh_marginal_filename, individuals_marginal_filename, individuals_data, households_data)
  
		#Dropping columns which lead to error
		household_marginal.drop(['num_workers','hhsize'], axis=1, errors='ignore', inplace=True)
		households_data.drop(['num_workers','hhsize'], axis=1, errors='ignore', inplace=True)

		synthetic_households, synthetic_individuals, synthetic_stats = synthesize_all_zones(household_marginal, individuals_marginal, households_data, individuals_data, xwalk)
		synthetic_households['household_id'] = synthetic_households.index
		return synthetic_households, synthetic_individuals, synthetic_stats

class PopulationDensitySampler:
	def __init__(self, population_density_filename):
		self.population_density_data = pd.read_csv(population_density_filename)
		columns_rename = {"X":"longitude", "Y":"latitude", "Z":"population_density"}
		self.population_density_data['X'] = self.population_density_data['X'].round(6)
		self.population_density_data['Y'] = self.population_density_data['Y'].round(6)
		self.population_density_data.rename(columns_rename, axis=1, inplace=True)
		self.population_density_data['point_object'] = self.population_density_data.progress_apply(lambda x : Point(x['longitude'], x['latitude']), axis=1)

	def add_point(self, latitude, longitude):
		distances = pow(self.population_density_data['latitude']-latitude, 2) + pow(self.population_density_data['longitude']-longitude,2)
		sorted_df = self.population_density_data.loc[distances.sort_values().index]
		mean_population_density = sorted_df.iloc[:4]['population_density'].mean()
		
		new_row_index = len(self.population_density_data)
		
		self.population_density_data.at[new_row_index, 'longitude'] =  longitude
		self.population_density_data.at[new_row_index, 'latitude'] = latitude
		self.population_density_data.at[new_row_index, 'population_density'] = mean_population_density
		self.population_density_data.at[new_row_index, 'point_object'] = Point(longitude, latitude)

	def get_lat_long_samples(self, n, polygon):
		subset = self.population_density_data[self.population_density_data['point_object'].progress_apply(polygon.contains)]
		
		if(len(subset)==0):
			raise Exception("No points within the given polygon")
		
		sample = subset.sample(weights='population_density', n=(n*10), replace=True).copy()
		
		sample.reset_index(drop=True, inplace=True)
		
		sample['latitude'] = sample['latitude'] + np.random.uniform(-0.015, 0.015, size=sample.shape[0])
		
		sample['longitude'] = sample['longitude'] + np.random.uniform(-0.015, 0.015, size=sample.shape[0])
		
		points = sample.progress_apply(lambda x : Point(x['longitude'], x['latitude']), axis=1)
		
		contained = points.progress_apply(polygon.contains)
		
		return sample[contained][['longitude', 'latitude']].sample(n, replace=True).values

class HLatHlongAgeAddition:
	def __init__(self, admin_units_geojson_filename, admin_units_population_filename, population_density_filename):
		self.admin_units = gpd.read_file(admin_units_geojson_filename)
		self.admin_units.sort_values(by='name', inplace=True)
		self.admin_units.reset_index(drop=True, inplace=True)
		self.population_density_sampler = PopulationDensitySampler(population_density_filename)

		self.admin_unit_wise_population = pd.read_csv(admin_units_population_filename)

		self.admin_unit_wise_population['lower_limit'] = self.admin_unit_wise_population['TOT_P'].cumsum()-admin_unit_wise_population['TOT_P']
		self.admin_unit_wise_population['upper_limit'] = self.admin_unit_wise_population['TOT_P'].cumsum()

		for admin_unit in self.admin_units.iterrows():
			admin_unit_centroid = admin_unit[1]['geometry'].centroid
			self.population_density_sampler.add_point(admin_unit_centroid.y, admin_unit_centroid.x)

		self.total_population = int(np.ceil(self.admin_unit_wise_population['TOT_P'].sum()/10000)*10000)

	def perform_transforms(self, synthetic_population, synthetic_households):
		synthetic_population['age'] = synthetic_population['age'].apply(lambda x : random.randint(80,95) if (x=="80p") else int(x.split("to")[0])) + np.random.randint(0,5,size=len(synthetic_population))

		synthetic_households['hhsize'] = synthetic_population.groupby('household_id').size()

		for admin_unit_wise_population_info in self.admin_unit_wise_population.iterrows():
			subset_index = (synthetic_households['hhsize'].cumsum()>=admin_unit_wise_population_info[1]['lower_limit']) & (synthetic_households['hhsize'].cumsum()<=admin_unit_wise_population_info[1]['upper_limit'])
			synthetic_households.loc[subset_index, 'AdminUnitName'] = admin_unit_wise_population_info[1]['Name']
			synthetic_households.loc[subset_index, 'AdminUnitLatitude'] = admin_unit_wise_population_info[1]['Latitude']
			synthetic_households.loc[subset_index, 'AdminUnitLongitude'] = admin_unit_wise_population_info[1]['Longitude']

		synthetic_households.dropna(inplace=True)

		synthetic_households[['H_Lat', 'H_Lon']] = None

		for admin_unit_name in synthetic_households['AdminUnitName'].unique():
			print(admin_unit_name)
			admin_unit_polygon = self.admin_units[self.admin_units['name']==admin_unit_name]['geometry'].iloc[0]
			admin_unit_houses_index = synthetic_households['AdminUnitName']==admin_unit_name
			n_houses_in_admin_unit = len(synthetic_households[admin_unit_houses_index])
			points = self.population_density_sampler.get_lat_long_samples(n_houses_in_admin_unit, admin_unit_polygon)
			synthetic_households.loc[admin_unit_houses_index, ['H_Lon', 'H_Lat']] = points

		synthetic_households.index.name = 'hh_index'

		columns_to_join = ['household_id', 'H_Lat', 'H_Lon', 'AdminUnitName', 'AdminUnitLatitude', 'AdminUnitLongitude']
		merged_df = pd.merge(synthetic_population, synthetic_households[columns_to_join] ,on='household_id')
		
		return merged_df