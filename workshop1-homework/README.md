# Data ingestion workshop with dlt
# Homework

#### Question 1: What is the sum of the outputs of the generator for limit = 5?
- A: 10.23433234744176
- B: 7.892332347441762
- **C: 8.382332347441762**
- D: 9.123332347441762

#### Answer 1:
> code:
```
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 5
generator = square_root_generator(limit)

sum = 0
for sqrt_value in generator:
    sum+=sqrt_value

print(f"The sum of the outputs of the generator for limit = 5 is: {sum}.")
```

> output:

> The sum of the outputs of the generator for limit = 5 is: 8.382332347441762.


#### Question 2: What is the 13th number yielded by the generator?
- A: 4.236551275463989
- **B: 3.605551275463989**
- C: 2.345551275463989
- D: 5.678551275463989

#### Answer 2:
> code:
```
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 13
generator = square_root_generator(limit)

counter = 1
for sqrt_value in generator:
    print(f"{counter}:{sqrt_value}")
    counter+=1
```

> output:

> 1:1.0

> ...

> 13:3.605551275463989.


#### Question 3: Append the 2 generators. After correctly appending the data, calculate the sum of all ages of people.
- **A: 353**
- B: 365
- C: 378
- D: 390

#### Answer 3:
> code:
```
import dlt
import duckdb

# Define your generators
def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

# Create a pipeline
pipeline = dlt.pipeline(
    pipeline_name='people',
    destination='duckdb',
    dataset_name='mydata',
)

# Load the first generator
load_info1 = pipeline.run(people_1(), table_name="people", write_disposition="replace")

# Create a connection to the duckdb database
conn = duckdb.connect(database='/content/people.duckdb', read_only=False)

# Calculate the sum of ages from the first generator and print it
sum_age1 = conn.execute("SELECT SUM(Age) FROM mydata.people").fetchall()[0][0]
print(f"The sum of ages from the first generator is: {sum_age1}")

# Append the second generator to the same table
load_info2 = pipeline.run(people_2(), table_name="people", write_disposition="append")

# Calculate the sum of ages after appending the second generator and print it
sum_age_after_append = conn.execute("SELECT SUM(Age) FROM mydata.people").fetchall()[0][0]
print(f"The sum of ages after appending the second generator is: {sum_age_after_append}")

```

> output:

> The sum of ages from the first generator is: 140

> The sum of ages after appending the second generator is: 353


#### Question 4: Merge the 2 generators using the ID column. Calculate the sum of ages of all the people loaded as described above.
- A: 215
- **B: 266**
- C: 241
- D: 258

#### Answer 4:
> code:
```
import dlt
import duckdb

# Define your generators
def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}
def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}
# Create a pipeline
pipeline = dlt.pipeline(
    pipeline_name='people',
    destination='duckdb',
    dataset_name='mydata',
)

# Load the first generator
load_info1 = pipeline.run(people_1(), table_name="people", write_disposition="replace")

# Create a connection to the duckdb database
conn = duckdb.connect(database='/content/people.duckdb', read_only=False)

# Append the second generator to the same table
load_info2 = pipeline.run(people_2(), table_name="people", write_disposition="merge")

# Calculate the sum of ages after appending the second generator and print it
sum_age_after_append = conn.execute("SELECT SUM(Age) FROM mydata.people").fetchall()[0][0]
print(f"The sum of ages after merging the second generator is: {sum_age_after_append}")
```

> output:

> The sum of ages after merging the second generator is: 266