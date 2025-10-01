#!/usr/bin/env python3
import argparse
import pandas as pd
from datetime import datetime
import sys
from collections import defaultdict
import gc

def process_csv_in_batches(input_file, start_date, end_date, chunk_size=10000):
    """
batching it

    """
    complaint_counts = defaultdict(int)
    total_rows = 0
    filtered_rows = 0
    
    try:
        chunk_iter = pd.read_csv(input_file, chunksize=chunk_size)
        
        for chunk_num, chunk in enumerate(chunk_iter, 1):
            print(f"Processing chunk {chunk_num}...", end='\r')
            
            date_col = chunk.columns[1]
            
            chunk[date_col] = pd.to_datetime(chunk[date_col], errors='coerce') #turn the date into more readable format
            
            mask = (chunk[date_col] >= start_date) & (chunk[date_col] <= end_date)
            filtered_chunk = chunk[mask] #essentially only keeping the columns that are within the prescribed date ranges
            
            total_rows += len(chunk) #for rbbustned
            filtered_rows += len(filtered_chunk)
            
            for _, row in filtered_chunk.iterrows():
                complaint_type = row.get('Complaint Type', 'Unknown')
                borough = row.get('Borough', 'Unknown')
                key = (complaint_type, borough) #hashmap inspired 
                complaint_counts[key] += 1 #save this for later
            #garbage collectorrrrrr
            del chunk, filtered_chunk
            gc.collect()
        
        print(f"\nProcessed {total_rows} total rows, {filtered_rows} filtered rows")
        
        result_data = []
        for (complaint_type, borough), count in complaint_counts.items():
            result_data.append({
                'Complaint Type': complaint_type,
                'Borough': borough,
                'Count': count
            })
        
        return pd.DataFrame(result_data), filtered_rows, total_rows
        
    except Exception as e:
        print(f"Error processing file: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='Filter CSV by date range (batch processing)')
    parser.add_argument('-i', '--input', required=True, help='Input CSV file')
    parser.add_argument('-s', '--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('-e', '--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('-o', '--output', help='Output CSV file (optional)')
    parser.add_argument('-c', '--chunk-size', type=int, default=10000, 
                       help='Chunk size for batch processing (default: 10000)')

    args = parser.parse_args()
    
    try:
        start_date = pd.to_datetime(args.start)
        end_date = pd.to_datetime(args.end)
        
        print(f"Processing {args.input} in batches of {args.chunk_size} rows...")
        print(f"Date range: {args.start} to {args.end}")
        
        # Process file in batches
        result_df, filtered_rows, total_rows = process_csv_in_batches(
            args.input, start_date, end_date, args.chunk_size
        )
        
        if args.output:
            result_df.to_csv(args.output, index=False)
            print(f"Filtered data saved to {args.output}")
        else:
            print("\nResults:")
            print(result_df.to_string())
        
        print(f"Filtered {filtered_rows} rows from {total_rows} total rows")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
