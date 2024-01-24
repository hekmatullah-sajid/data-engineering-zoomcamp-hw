
**Module 1 Homework**

**Docker & SQL**

In this homework we'll prepare the environment and practice with Docker and SQL

**Question 1. Knowing docker tags**

Run the command to get information on Docker

docker --help

Now run the command to get help on the "docker build" command:

docker build --help

Do the same for "docker run".

Which tag has the following text? - _Automatically remove the container when it exits_

- --delete
- --rc
- --rmc
- **--rm**

**Question 2. Understanding docker first run**

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed ( use pip list ).

What is version of the package _wheel_ ?

- **0.42.0**
- 0.0
- 0.1
- 1.0

**Prepare Postgres**

Run Postgres and load data as shown in the videos We'll use the green taxi trips from September 2019:

wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green\_tripdata\_2019-09.csv.gz

You will also need the dataset with zones:

wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+\_zone\_lookup.csv

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

**Question 3. Count records**

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18.

Remember that lpep\_pickup\_datetime and lpep\_dropoff\_datetime columns are in the format timestamp (date and hour+min+sec) and not in date.

- 15767
- **15612**
- 15859
- 89009

**Query:**

```
SELECT COUNT(1) FROM green\_taxi\_trips WHERE DATE(lpep\_pickup\_datetime) = '2019-09-18' AND DATE(lpep\_dropoff\_datetime) = '2019-09-18'
```

**Question 4. Largest trip for each day**

Which was the pick up day with the largest trip distance Use the pick up time for your calculations.

- 2019-09-18
- 2019-09-16
- **2019-09-26**
- 2019-09-21

**Query:** 

```
SELECT trip\_distance, lpep\_pickup\_datetime
FROM green\_taxi\_trips
ORDER BY trip\_distance DESC
LIMIT 1;
```

**Question 5. The number of passengers**

Consider lpep\_pickup\_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total\_amount superior to 50000?

- **"Brooklyn" "Manhattan" "Queens"**
- "Bronx" "Brooklyn" "Manhattan"
- "Bronx" "Manhattan" "Queens"
- "Brooklyn" "Queens" "Staten Island"

**Query:**

```
SELECT tz."Borough", SUM(gt.total\_amount) AS count\_num
FROM green\_taxi\_trips AS gt
INNER JOIN taxi\_zones AS tz ON gt."PULocationID" = tz."LocationID"
WHERE DATE(gt.lpep\_pickup\_datetime) = '2019-09-18'
GROUP BY tz."Borough"
HAVING SUM(gt.total\_amount) \> 50000
ORDER BY count\_num DESC;
```

**Question 6. Largest tip**

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id.

Note: it's not a typo, it's tip , not trip

- Central Park
- Jamaica
- **JFK Airport**
- Long Island City/Queens Plaza

**Query:**

```
WITH pickups\_astoria AS
(
SELECT \*
FROM green\_taxi\_trips AS gt
INNER JOIN taxi\_zones AS tz ON gt."PULocationID" = tz."LocationID"
WHERE tz."Zone" = 'Astoria'
)
SELECT tz."Zone", gt.tip\_amount
FROM pickups\_astoria AS gt
INNER JOIN taxi\_zones AS tz ON gt."DOLocationID" = tz."LocationID"
WHERE EXTRACT(MONTH FROM gt.lpep\_pickup\_datetime) = 9
AND EXTRACT(YEAR FROM gt.lpep\_pickup\_datetime) = 2019
ORDER BY gt.tip\_amount DESC
LIMIT 1;
```

**Terraform**

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. Copy the files from the course repo [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.

**Question 7. Creating Resources**

After updating the main.tf and variable.tf files run:

terraform apply

Paste the output of this command into the homework submission form.

Output:

```
Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following

symbols:

+ create

Terraform will perform the following actions:

# google\_bigquery\_dataset.demo\_dataset will be created

+ resource "google\_bigquery\_dataset" "demo\_dataset" {

+ creation\_time = (known after apply)

+ dataset\_id = "IaC\_dataset"

+ default\_collation = (known after apply)

+ delete\_contents\_on\_destroy = false

+ effective\_labels = (known after apply)

+ etag = (known after apply)

+ id = (known after apply)

+ is\_case\_insensitive = (known after apply)

+ last\_modified\_time = (known after apply)

+ location = "US"

+ max\_time\_travel\_hours = (known after apply)

+ project = "hs-dez-gcp-2024-project"

+ self\_link = (known after apply)

+ storage\_billing\_model = (known after apply)

+ terraform\_labels = (known after apply)

}

# google\_storage\_bucket.demo-bucket will be created

+ resource "google\_storage\_bucket" "demo-bucket" {

+ effective\_labels = (known after apply)

+ force\_destroy = true

+ id = (known after apply)

+ location = "US"

+ name = "iac-hs-dez-bucket-2024"

+ project = (known after apply)

+ public\_access\_prevention = (known after apply)

+ self\_link = (known after apply)

+ storage\_class = "STANDARD"

+ terraform\_labels = (known after apply)

+ uniform\_bucket\_level\_access = (known after apply)

+ url = (known after apply)

+ lifecycle\_rule {

+ action {

+ type = "AbortIncompleteMultipartUpload"

}

+ condition {

+ age = 1

+ matches\_prefix = []

+ matches\_storage\_class = []

+ matches\_suffix = []

+ with\_state = (known after apply)

}

}

}

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?

Terraform will perform the actions described above.

Only 'yes' will be accepted to approve.

Enter a value: yes

google\_bigquery\_dataset.demo\_dataset: Creating...

google\_storage\_bucket.demo-bucket: Creating...

google\_bigquery\_dataset.demo\_dataset: Creation complete after 1s [id=projects/hs-dez-gcp-2024-project/datasets/IaC\_dataset]

google\_storage\_bucket.demo-bucket: Creation complete after 3s [id=iac-hs-dez-bucket-2024]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```
