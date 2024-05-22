import sys
import pandas as pd

# Run in command line: python3 flag-column-script.py example_input_data.csv example_abp_data.csv 

abp_data = pd.read_csv(sys.argv[2])
input_data = pd.read_csv(sys.argv[1])

# This implementation checks if any of the address lines appear in either STREET_NAME or SINGLE_LINE_ADDRESS. It can be easily modified to only check STREET_NAME.
# To optimise for large datasets the data can be loaded in using PySpark.

def check_address(postcode, address_lines, abp_data):
    """
    Check if any of the address lines appear in the STREET_NAME or SINGLE_LINE_ADDRESS columns of abp_data for a given postcode.

    Parameters:
    postcode (str): The postcode to match in the abp_data DataFrame.
    address_lines (list): A list of address lines to check against the STREET_NAME and SINGLE_LINE_ADDRESS columns.
    abp_data (DataFrame): The DataFrame containing SINGLE_LINE_ADDRESS, POSTCODE, and STREET_NAME columns.

    Returns:
    str: 'Yes' if any address line appears in STREET_NAME for the same postcode, 'No' otherwise.
    """
    matching_rows = abp_data[abp_data['POSTCODE'] == postcode]
    for _, row in matching_rows.iterrows():
        street_name = row['STREET_NAME']
        full_address = row['SINGLE_LINE_ADDRESS']
        if pd.notna(street_name):  # Check if street_name is not NaN
            for line in address_lines:
                if (line.upper() in street_name.upper()) or (line.upper() in full_address.upper()): # Return 'Yes' if in either STREET_NAME or SINGLE_LINE_ADDRESS
                    return 'Yes'
        # If STREET_NAME is NaN but correct address is in SINGLE_LINE_ADDRESS return 'Yes'
        elif pd.isna(street_name):
            for line in address_lines:
                if line.upper() in full_address.upper():
                    return 'Yes'
    return 'No'

def create_street_in_postcode_col(input_data, abp_data):
    """
    Update the input_data DataFrame with a 'Street_In_Postcode' column indicating if any address lines appear in STREET_NAME or SINGLE_LINE_ADDRESS for the same postcode.

    Parameters:
    input_data (DataFrame): The DataFrame containing address lines and postcodes.
    abp_data (DataFrame): The DataFrame containing SINGLE_LINE_ADDRESS, POSTCODE, and STREET_NAME columns.

    Returns:
    DataFrame: The updated input_data DataFrame with the 'Street_In_Postcode' column.
    """
    input_data['Street_In_Postcode'] = input_data.apply(
        lambda row: check_address(
            row['Postcode'],
            [str(line) if pd.notna(line) else '' for line in [
            row['Address_Line_1'], row['Address_Line_2'], row['Address_Line_3'], row['Address_Line_4'], row['Address_Line_5']
        ]],
            abp_data
        ),
        axis=1
    )
    return input_data

input_data = create_street_in_postcode_col(input_data, abp_data)
input_data.to_csv('example_output_data.csv')

