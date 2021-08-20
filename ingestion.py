#This module pulls data from the constituent, email, and subscription csvs and creates a single "people" csv 

import pandas

CONSTITUENTS_FILE="https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv"
EMAIL_FILE="https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv"
SUBSCRIPTION_FILE="https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv"

if __name__=="__main__":

	#pull in constituents data
	constituents=pandas.read_csv(CONSTITUENTS_FILE)

	#drop unnecessary columns 
	constituents=constituents[["cons_id","source","create_dt","modified_dt"]]

	#read emails data
	emails=pandas.read_csv(EMAIL_FILE)
	
	#drop unneeded columns
	emails=emails[["cons_id","cons_email_id","email","is_primary"]]

	#limit to primary enails
	emails=emails[emails.is_primary==1].drop(columns=["is_primary"])

	# #drop unneeded column
	# emails=emails.drop(columns=["is_primary"])

	#use a right join-- we don't want constituents without emails since they won't have an identifier in the output table
	#but we should include all emails and let downstream sources decide how to treat rows with NULL non-primary fields
	cons_w_email=pandas.merge(constituents,emails,on="cons_id", how="right")

	subscription=pandas.read_csv(SUBSCRIPTION_FILE)
	#drop unneeded columns
	subscription=subscription[["cons_email_id","isunsub","chapter_id"]]
	#limit to chapter_id of 1, then drop column
	subscription=subscription[subscription.chapter_id==1].drop(columns=["chapter_id"])

	#merge subscription info into result data frame
	cons_w_email=pandas.merge(cons_w_email,subscription, on="cons_email_id", how="left")
	#fillNa with 0 to specify that if a email is not present, it's assumed to be still subscribed
	cons_w_email["isunsub"]=cons_w_email["isunsub"].fillna(0)
	cons_w_email=cons_w_email.drop(columns=["cons_id","cons_email_id"])

	#update schema to match spec
	cons_w_email=cons_w_email.rename(columns={"source" : "code", "create_dt" :"created_dt", "modified_dt":"updated_dt", "isunsub":"is_unsub"})
	
	#write the file
	cons_w_email.to_csv("./people.csv", header=True, index=False, na_rep="NULL")








