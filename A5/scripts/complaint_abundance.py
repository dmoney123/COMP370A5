import argparse
import pandas as pd

def main():
    parser = argparse.ArgumentParser(description = "determine abundant complaint")
    parser.add_argument('-i', '--input', required = True, help = 'input CSV')
    parser.add_argument('-o', '--ouput', required = False, help = 'input output file name')

    args = parser.parse_args()

    # Read the CSV file
    df = pd.read_csv(args.input)
    
    # Count complaint types, then sort by count
    complaint_counts = df.groupby('Complaint Type')['Count'].sum().sort_values(ascending=False)
    
    # Find the most abundant
    most_abundant = complaint_counts.index[0]
    count = complaint_counts.iloc[0]


    
    print(f"Most abundant complaint type: {most_abundant}")
    print(f"Count: {count}")
    
    # Show top 5 for context
    print("\nTop 5 complaint types:")
    print(complaint_counts.head())


if __name__  == "__main__":
    main()


