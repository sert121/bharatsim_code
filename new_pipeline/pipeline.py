from synthpop_steps import IPU, HLatHlongAgeAddition

###Configuration

#Set source for marginals and samples from IHDS survey
householdh_marginal_filename = '/content/drive/MyDrive/data/aastha synthetic/household_marg.csv'
individuals_marginal_filename = '/content/drive/MyDrive/data/aastha synthetic/person_marg.csv'

ihds_individuals_filename = "/content/drive/MyDrive/data/aastha synthetic/36151-0001-Data.tsv"
ihds_household_filename = "/content/drive/MyDrive/data/aastha synthetic/36151-0002-Data.tsv"

#The geojson must have ward names as well. This should match with the population file defined below
admin_units_geojson_filename = "https://raw.githubusercontent.com/datameet/Municipal_Spatial_Data/master/Mumbai/BMC_Wards.geojson"

#From https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km/2020/IND/ind_pd_2020_1km_ASCII_XYZ.zip unzip this
population_density_filename = "ind_pd_2020_1km_ASCII_XYZ.csv"

#I write to this file but it can be a CSV as well
admin_units_population_filename = "mumbai_ward_wise_pop.csv" 
 
# select state number and name appropriately
state_id = 27 

#Select distid within state
district_ids = [22, 23] #Mumbai Suburban and Mumbai Urban

output_file_name = 'mumbai.csv'
###Configuration
 
###Custom operation for mumbai. This data can be read from a csv
ward_wise_pop = """Ward A   18.9337657  72.8364969  185014
Ward B  18.9614551  72.8333212  127290
Ward C  18.946123   72.8249009  166161
Ward D  18.9626147  72.813162   346866
Ward E  18.9718778  72.8313269  393286
Ward F North    19.0294197  72.8546058  529034
Ward F South    19.0058779  72.8396881  360972
Ward G North    19.023175   72.8434324  599039
Ward G South    19.0083734  72.8304087  377749
Ward H East 19.0851064  72.8445455  557239
Ward H West 19.0561063  72.8352939  307581
Ward K East 19.1200923  72.8523868  823885
Ward K West 19.1195001  72.844486   748688
Ward L  19.0704672  72.8790936  902225
Ward M East 19.0564771  72.9215464  807720
Ward M West 19.0611012  72.8993043  411893
Ward N  19.0839316  72.9064422  622853
Ward P North    19.1877853  72.8423072  941366
Ward P South    19.1626595  72.8464575  463507
Ward R Central  19.2311189  72.8558279  562162
Ward R North    19.1200923  72.8523868  431368
Ward R South    19.2039634  72.8453958  691229
Ward S  19.1390295  72.9304517  743783
Ward T  19.1756249  72.950922   341463""".split("\n")
admin_unit_wise_population = pd.DataFrame(list(map(lambda x : x.split("\t"), ward_wise_pop)), columns = ['Name', 'Latitude', 'Longitude', 'TOT_P'])
admin_unit_wise_population['Name'] = admin_unit_wise_population['Name'].apply(lambda x : x[x.find(" ")+1:].replace(" North", "/N").replace(" South", "/S").replace(" East", "/E").replace(" West", "/W").replace(" Central", "/C"))
admin_unit_wise_population.sort_values(by='Name', inplace=True)
admin_unit_wise_population.reset_index(drop=True, inplace=True)
admin_unit_wise_population['TOT_P'] = admin_unit_wise_population['TOT_P'].apply(int)
admin_unit_wise_population.to_csv(admin_units_population_filename, index=False)
###Custom operation for mumbai ends


###Reading and filtering survey sample data
ihds_individuals_data = pd.read_csv(ihds_individuals_filename, sep='\t')

filtered_ihds_individuals_data = ihds_individuals_data.loc[ihds_individuals_data.STATEID==state_id]

filtered_ihds_individuals_data = filtered_ihds_individuals_data[filtered_ihds_individuals_data['DISTID'].apply(lambda x : x in district_ids)]

ihds_households_data = pd.read_csv("/content/drive/MyDrive/data/aastha synthetic/36151-0002-Data.tsv", sep='\t')

filtered_ihds_households_data = ihds_households_data.loc[ihds_households_data.STATEID==state_id] 

filtered_ihds_households_data = filtered_ihds_households_data[filtered_ihds_households_data['DISTID'].apply(lambda x : x in district_ids)]
###Reading and filtering survey sample data

###IPU Step
ipu_object = IPU()

#passing all requirements to the IPU function
synthetic_households, synthetic_individuals, synthetic_stats = ipu_object.generate_data(filtered_ihds_individuals_data, filtered_ihds_households_data, householdh_marginal_filename, individuals_marginal_filename)
###IPU Step

###Adding hlat hlong and age
hlat_hlong_age_object = HLatHlongAgeAddition(admin_units_geojson_filename, admin_units_population_filename, population_density_filename)

#passing base population and getting the joined population with newly added columns [AdminUnitName, AdminUnitLat, AdminUnitLong, household_id, h_lat, h_long]
#also age is now in numbers rather than in bins
new_synthetic_population = hlat_hlong_age_object.perform_transforms(synthetic_individuals, synthetic_households)

#saving file
new_synthetic_population.to_csv(output_file_name, index=False)
###Adding hlat hlong and age
