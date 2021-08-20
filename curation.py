#this module creates the acquisition_facts table

import pandas

PEOPLE_FILE="./people.csv"

if __name__=="__main__":
	people=pandas.read_csv(PEOPLE_FILE)

	#get creation date without time
	people['created_dt']=pandas.to_datetime(people['created_dt']).dt.date

	#do the grouping
	acquisitions=people.groupby(["created_dt"]).size().reset_index(name='acquisitions')

	#schema update
	acquisitions=acquisitions.rename(columns={"created_dt":"acquisition_date"})

	#generate the file
	acquisitions.to_csv("./acquistion_facts.csv", header=True, index=False)

