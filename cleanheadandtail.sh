#!/bin/bash -x

# Check if file has empty last line
file_has_empty_last_line() {
    local file="$1"
    local last_line=$(tail -n 1 "$file")
    [[ -z "$last_line" ]]
}

# Remove empty last line from file
remove_empty_last_line() {
    local file="$1"
    sed -i '$ { /^$/d; }' "$file"
}

# Remove header lines from file
remove_header_lines() {
    local file="$1"
    local header_lines="$2"
    sed -i "1,${header_lines}d" "$file"
}

# Remove trailer lines from file
# remove_trailer_lines() {
#     local file="$1"
#     local trailer_lines="$2"
#     sed -i "${trailer_lines}q" "$file"
# }

remove_trailer_lines() {
    local file="$1"
    local trailer_lines="$2"
    #sed -i "${trailer_lines}q" "$file"
    #line_count=$(wc -l < "$file")
    head -n -$2 $file > $$_tmp.txt && mv $$_tmp.txt $file
   
}

# Main script logic
main() {
    local file="$1"
    local header_lines="$2"
    local trailer_lines="$3"

    echo $file

    head -1 $file
    tail -1 $file

    if file_has_empty_last_line "$file"; then
        remove_empty_last_line "$file"
    fi

    head -1 $file
    tail -1 $file

    if [[ "$header_lines" -gt 0 ]]; then
        remove_header_lines "$file" "$header_lines"
    fi

    head -1 $file
    tail -1 $file

    if [[ "$trailer_lines" -gt 0 ]]; then
        remove_trailer_lines "$file" "$trailer_lines"
    fi

    head -1 $file
    tail -1 $file
    sort -o $file $file
    rm -f $file.gz
    gzip -v $file
}




# Usage: cleanheadandtail.sh <filename> <number_of_header_lines> <number_of_trailer_lines>
main "$@"
