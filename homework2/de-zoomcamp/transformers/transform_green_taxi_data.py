import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    print(f"The shape of the dataset before transformation is: {data.shape}.")

    print(f"There are {data['passenger_count'].isin([0]).sum()} records with zero passengers.")

    print(f"There are {data['passenger_count'].isnull().sum()} records with null passengers.")

    print(f"There are {data['trip_distance'].isin([0]).sum()} records with zero trip distance.")

    # Removing records with zero passengers and zero trip distance
    data = data[data['passenger_count']>0]
    data = data[data['trip_distance']>0]

    # Creating a date column for partitioning by date
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    print(f"There are {len(data)} rows in the dataset after transformation.")

    VendorID_values = pd.unique(data['VendorID']).tolist()
    print(f"The existing values of VendorID in the dataset are: {VendorID_values}.")

    # Column names transformation from CamelCase to snake_case
    data.columns = (data.columns
                    .str.replace('ID','_id')
                    .str.replace('PU','pu_')
                    .str.replace('DO','do_')
                    .str.lower()
    )

    return data

# Performing test, no records with zero passengers
@test
def test_passenger_count(output, *args):

    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers.'

# Performing test, no records with zero trip distance
@test
def test_trip_distance(output, *args):

    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero trip distance.'

# Performing test, no records with vendor_id outside existing vendor_id values
@test
def test_VendorID(output, *args):

    assert output['vendor_id'].isin([1,2]).all(), 'There are rides with with none existing VendorID values.'

