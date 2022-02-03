import unicodedata
import re
import requests
import pandas as pd

def slugify(value, allow_unicode=False):
	# Taken from https://github.com/django/django/blob/master/django/utils/text.py
	value = str(value)
	if allow_unicode:
		value = unicodedata.normalize("NFKC", value)
	else:
		value = unicodedata.normalize("NFKD", value).encode(
			"ascii", "ignore").decode("ascii")
	value = re.sub(r"[^\w\s-]", "", value.lower())
	return re.sub(r"[-\s]+", "-", value).strip("-_")

counties = ["Nassau", "Suffolk"]

# We are defining the Socrata datasets we want to scrape here.
datasets = [
	{
		"identifier": "xubh-q36u",
		# "title": "Hospital General Information",
		# "dataset_link": "https://data.cms.gov/provider-data/dataset/xubh-q36u",
		"description": "Hospitals registered with Medicare"
	}, {
		"identifier": "ynj2-r877",
		# "title": "Complications and Deaths - Hospital",
		# "dataset_link": "https://data.cms.gov/provider-data/dataset/ynj2-r877",
		"description": "Complication and death rates for hospitals registered with Medicare"
	}, {
		"identifier": "632h-zaca",
		# "title": "Unplanned Hospital Visits - Hospital",
		# "dataset_link": "https://data.cms.gov/provider-data/dataset/632h-zaca",
		"description": "Unplanned visit rates for hospitals registered with Medicare"
	}, {
		"identifier": "77hc-ibv8",
		# "title": "Healthcare Associated Infections - Hospital",
		# "dataset_link": "https://data.cms.gov/provider-data/dataset/77hc-ibv8",
		"description": "Infection cases in hospitals registered with Medicare"
	}, {
		"identifier": "c7us-v4mf",
		# "title": "Payment and value of care - Hospital",
		# "dataset_link": "https://data.cms.gov/provider-data/dataset/c7us-v4mf",
		"description": "Average patient payments for hospitals registered with Medicare"
	}, {
		"identifier": "dgck-syfz",
		# "title": "Patient survey (HCAHPS) - Hospital",
		# "dataset_link": "https://data.cms.gov/provider-data/dataset/dgck-syfz",
		"description": "CAHPS patient surveys for hospitals registered with Medicare"
	}, {
		"identifier": "yv7e-xc69",
		# "title": "Timely and Effective Care - Hospital",
		# "dataset_link": "https://data.cms.gov/provider-data/dataset/yv7e-xc69",
		"description": "Quality measures for hospitals registered with Medicare"
	}, {
		"identifier": "rrqw-56er",
		# "title": "Medicare Spending Per Beneficiary - Hospital",
		# "dataset_link": "https://data.cms.gov/provider-data/dataset/rrqw-56er",
		"description": "Medicare hospital spending per patient"
	}, {
		"identifier": "wkfw-kthe",
		# "title": "Outpatient Imaging Efficiency - Hospital",
		# "dataset_link": "https://data.cms.gov/provider-data/dataset/wkfw-kthe",
		"description": "Use of medical imaging in hospitals registered with Medicare"
	}
]

# We are going to call every item within "datasets" a "dataset". As we go through each dataset, we are going to scrape the dataset.
for dataset in datasets:
	try:
		# We are creating an empty list called "results".
		results = []
		# We are going to call every item within "counties" a "county". As we go through each county, we are going to scrape the dataset for that county.
		for county in counties:
			url = "https://data.cms.gov/provider-data/api/1/datastore/query/" + dataset["identifier"] + "/0"
			# The limit can be up to 500.
			limit = 500
			# Start the offset at 0.
			offset = 0
			initial_payload = "limit=" + str(limit) + "&offset=" + str(offset) + "&count=true&results=true&schema=true&keys=true&format=json&rowIds=false&conditions%5B0%5D%5Bproperty%5D=state&conditions%5B0%5D%5Boperator%5D=%3D&conditions%5B0%5D%5Bvalue%5D=NY&conditions%5B1%5D%5Bproperty%5D=county_name&conditions%5B1%5D%5Boperator%5D=%3D&conditions%5B1%5D%5Bvalue%5D=" + county
			# "requests" documentation page is here: https://docs.python-requests.org/en/master/user/quickstart/
			initial_request = requests.get(url, params=initial_payload)
			# As we go through each page of the dataset, we are going to scrape that page of the dataset.
			count = initial_request.json()["count"]
			i = 0
			while i < count / limit:
				offset = i * limit
				loop_payload = "limit=" + str(limit) + "&offset=" + str(offset) + "&count=true&results=true&schema=true&keys=true&format=json&rowIds=false&conditions%5B0%5D%5Bproperty%5D=state&conditions%5B0%5D%5Boperator%5D=%3D&conditions%5B0%5D%5Bvalue%5D=NY&conditions%5B1%5D%5Bproperty%5D=county_name&conditions%5B1%5D%5Boperator%5D=%3D&conditions%5B1%5D%5Bvalue%5D=" + county
				loop_request = requests.get(url, params=loop_payload)
				for result in loop_request.json()["results"]:
					results.append(result)
				i += 1
		# "pandas" documentation page is here: https://pandas.pydata.org/docs/index.html
		df = pd.DataFrame(results)
		file_name = slugify(dataset["description"])
		df.to_csv("csv/" + file_name + ".csv", index=False)
	except:
		pass