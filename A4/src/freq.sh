#!/bin/bash

input_file="/home/dmy/A4/data/clean_dialog.csv"
output_file="/home/dmy/A4/data/Line_percentage.csv"
total_lines=36859
names=("Fluttershy" "Twilight Sparkle" "Rarity" "Pinkie Pie" "Rainbow Dash")

# Extract the 3rd CSV column (pony) safely; requires grep -P
# before the tail | grep ... pipeline
tmp="$(mktemp)"; trap 'rm -f "$tmp"' EXIT

tail -n +2 "$input_file" \
| grep -oP '^(?:"(?:[^"]|"")*",){2}\K"(?:[^"]|"")*"' \
| sed 's/^"//;s/"$//' > "$tmp"

echo "pony_name,total_line_count,percent_all_lines" > "$output_file"
for name in "${names[@]}"; do

  count=$(grep -ci -- "$name" "$tmp")

  percent=$(awk -v c="$count" -v t="$total_lines" 'BEGIN{ if (t==0) printf "0"; else printf "%.4f", 100*c/t }')

  echo "\"$name\",$count,$percent" >> "$output_file"
done

echo "Wrote $output_file"
