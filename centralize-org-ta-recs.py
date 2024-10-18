import boto3
import json
import sys
import io
import datetime 
import csv

#Globals
S3_BUCKET = "sm-ta-api"
S3_FOLDER = "centralized_ta_recommendations/"


sts_client = boto3.client('sts')


def LINE():
    return sys._getframe(1).f_lineno
    
# Define a custom function to serialize datetime objects 
def serialize_datetime(obj): 
    if isinstance(obj, datetime.datetime): 
        return obj.isoformat() 
    raise TypeError("Type not serializable") 


# Upload file to S3
def upload_file_to_s3(file_path, bucket_name, s3_key):
    
    # Create S3 client
    s3 = boto3.client('s3')

    try:
        # Upload the file to S3
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"File '{file_path}' uploaded to S3 bucket '{bucket_name}' with key '{s3_key}'.")
    except Exception as e:
        print(f"Error uploading file '{file_path}' to S3 bucket '{bucket_name}': {e}")

# PutObject to S3
def put_object_to_s3(data, bucket_name, s3_key):
    try:
    	# Put Object to S3
    	s3_client = boto3.client('s3')
    	s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=data)
    	print(f"File '{s3_key}' uploaded to S3 bucket '{bucket_name}' with key '{s3_key}'.")  
    except Exception as e:
        print(f"Error uploading file '{s3_key}' to S3 bucket '{bucket_name}': {e}")
    
    return

# Get accounts in Org
def get_org_accounts():
	org_client = boto3.client('organizations')
	acct_list = org_client.list_accounts()
	active_accts = []
	for accts in  acct_list['Accounts']:
	   if accts['Status'] == 'ACTIVE':     
	      active_accts.append(accts['Id'])
	
	return active_accts


# Assume cross-account role 
def assume_role(role_arn, role_session_name):
	try:
		resp = sts_client.assume_role(
						RoleArn= role_arn,
						RoleSessionName= role_session_name)
		secretKey = resp['Credentials']['SecretAccessKey']
		accessKey = resp['Credentials']['AccessKeyId']
		sessionToken = resp['Credentials']['SessionToken']
		
	except ClientError as error:
	       print( 'Unexpected error occurred... could not assume role', error )
	       return 400

	return {"secretKey":secretKey, "accessKey":accessKey, "sessionToken":sessionToken}



# Extract recommendations as CSV
def extract_recommendations(recs):
	try:	
		#print(recs)
		#f = open(file, "a", newline='')
		#writer = csv.writer(f)
		
		# Create an in-memory text stream and CSV output
		output = io.StringIO()
		csv_writer = csv.writer(output)
		
		
		rec_summaries = recs['recommendationSummaries']
				
		for rec in rec_summaries:
			acct_id = rec['arn'].split(':')[4]
			check_id = rec['checkArn'].split(':')[5].split('/')[1]
			last_updated_time = rec['lastUpdatedAt']
			check_name = rec['name']
			rec_pillar = rec['pillars'][0]
			rec_source = rec['source']
			rec_status = rec['status']
			rec_type = rec['type']
			try:
				aws_service = rec['awsServices'][0]
				#print("Record = ", acct_id, aws_service, check_id)
				#print("Record = ", acct_id, aws_service, check_id, last_updated_time, check_name, rec_pillar, rec_source, rec_status, rec_type)
			except:
				#print("")
				#print("MISSING AWS SERVICE = ", rec['checkArn'])
				aws_service = "Unknown - " + rec['checkArn']
				#print("")
				
			#writer.writerow([acct_id, aws_service, check_id, last_updated_time, check_name, rec_pillar, rec_source, rec_status, rec_type])
			csv_writer.writerow([acct_id, aws_service, check_id, last_updated_time, check_name, rec_pillar, rec_source, rec_status, rec_type])				
		
		#f.close()
		
		csv_content = output.getvalue()
		
		# Close the StringIO object
		output.close()
		
		# Now csv_content holds the CSV as a string
		print(csv_content)
		
		return csv_content
		
	except Exception as e:
		print("Exception in function save_recommendations")
		print(e)

# Write recommendations to file
def save_recommendations(recs, file):
	try:	
		#print(recs)
		f = open(file, "a", newline='')
		writer = csv.writer(f)
	
		rec_summaries = recs['recommendationSummaries']
		#rec_summaries = recs
		
		for rec in rec_summaries:
			acct_id = rec['arn'].split(':')[4]
			check_id = rec['checkArn'].split(':')[5].split('/')[1]
			last_updated_time = rec['lastUpdatedAt']
			check_name = rec['name']
			rec_pillar = rec['pillars'][0]
			rec_source = rec['source']
			rec_status = rec['status']
			rec_type = rec['type']
			try:
				aws_service = rec['awsServices'][0]
				#print("Record = ", acct_id, aws_service, check_id)
				#print("Record = ", acct_id, aws_service, check_id, last_updated_time, check_name, rec_pillar, rec_source, rec_status, rec_type)
			except:
				#print("")
				#print("MISSING AWS SERVICE = ", rec['checkArn'])
				aws_service = "Unknown - " + rec['checkArn']
				#print("")
				
			writer.writerow([acct_id, aws_service, check_id, last_updated_time, check_name, rec_pillar, rec_source, rec_status, rec_type])
		
		f.close()
	
	except Exception as e:
		print("Exception in function save_recommendations")
		print(e)


# Defining main function
def main():
	
	active_accts = get_org_accounts()
	
	for acct in active_accts:
		try:
			## Format role_arn from account
			role_arn = "arn:aws:iam::" + acct + ":role/TrustedAdvisorAPIAccessRole"
			
			role_session = acct + "_ta_session"
			
			## Assume cross-account role in Organizations Account 
			role_creds = assume_role(role_arn, role_session)
			#print("Secret: ", role_creds["secretKey"], "Access: ", role_creds["accessKey"], "Token: ", role_creds["sessionToken"])
			
			## List TA Recommendations
			ta_client = boto3.client('trustedadvisor', region_name='us-east-1', aws_access_key_id=role_creds["accessKey"], aws_secret_access_key=role_creds["secretKey"], aws_session_token=role_creds["sessionToken"])
			
			#response = ta_client.list_recommendations(type='standard', maxResults=200)
			#ta_client = boto3.client('trustedadvisor', region_name='us-east-1')
			
			paginator = ta_client.get_paginator('list_recommendations')		
			response_iterator = paginator.paginate(    
								    type='standard',
								    PaginationConfig={
									'MaxItems': 1000,
									'PageSize': 100
								    }
								)
								
			file_name = "recs_" + acct + ".csv"
			#f = open(file_name, "w")
			#f.close
			
			output = ""
			
			for page in response_iterator:																 			
				#save_recommendations(page, file_name)
				output += extract_recommendations(page)
					
			bucket_name = S3_BUCKET
			s3_key = S3_FOLDER + file_name
			
			# ARGS: file_path, bucket_name, s3_key
			#upload_file_to_s3(file_name, bucket_name, s3_key)
			put_object_to_s3(output, bucket_name, s3_key)
			
			#json_data = json.dumps(response, default=serialize_datetime) 
			#print("RESPONSE =\n\n", json_data)			
		except Exception as e:
			print("Exception in function main() for account ", acct)
			print(e)
			
	return


if __name__=="__main__":
    main()


