#!/bin/bash -x



# Main script logic
main() {
    local file="$1"
    cp -p $file $file.fl
    sed -i 's#"##g' $file.fl
    sed -i 's#,##g' $file.fl
}




# Usage: cleanheadandtail.sh <filename> <number_of_header_lines> <number_of_trailer_lines>
main "$@"
