# Print help message
print_help() {
  echo "Usage: $0 [options] <suite_name>"
  echo ""
  echo "Options:"
  echo "  -h, --help            Display this help message"
  echo "  -d, --doris-home DIR  Set Doris home directory (default: $DORIS_HOME)"
  echo "  -s, --source-dir DIR  Set test files source directory (default: $TEST_FILES_DIR)"
  echo "  -t, --target-dir DIR  Set target directory (default: $TARGET_DIR)"
  # echo "  -o, --output-dir DIR  Set test output file directory (default: $OUTPUT_DIR)"
  echo ""
  echo "Environment variables:"
  echo "  DORIS_HOME            Set Doris home directory"
  echo "  TEST_FILES_DIR        Set test files source directory"
  echo "  TARGET_DIR            Set target directory"
  # echo "  OUTPUT_DIR            Set output file directory"
  echo ""
  echo "Example:"
  echo "  $0 test_suite          # Will look for test_suite.groovy in any subdirectory"
  echo "  $0 --doris-home /path/to/doris test_suite"
  echo "  $0 --source-dir /path/to/tests test_suite"
  echo ""
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      print_help
      ;;
    -d|--doris-home)
      DORIS_HOME="$2"
      shift 2
      ;;
    -s|--source-dir)
      TEST_FILES_DIR="$2"
      shift 2
      ;;
    -t|--target-dir)
      TARGET_DIR="$2"
      shift 2
      ;;
    # -o|--output-dir)
    #   OUTPUT_DIR="$2"
    #   shift 2
    #   ;;
    -*)
      echo "Unknown option: $1"
      print_help
      ;;
    *)
      # The first non-option argument is the suite name
      suite_name="$1"
      shift
      ;;
  esac
done

# Set default values only if not specified via command line
: ${DORIS_HOME:="/path/to/doris"}
: ${TEST_FILES_DIR:="$(pwd)/../regression-test"}
: ${TARGET_DIR:="$DORIS_HOME/regression-test/suites/ccr_test"}
: ${OUTPUT_DIR:="$TARGET_DIR/../../data/$(basename "$TARGET_DIR")"}

# Check if suite name is provided
if [ -z "$suite_name" ]; then
  echo "ERROR: Please provide a suite name as an argument."
  print_help
  exit 1
fi

# Always add .groovy extension for file lookup
test_file_name="${suite_name}.groovy"

test_output_name="${suite_name}.out"

# Find the test file recursively in TEST_FILES_DIR
found_files=$(find "$TEST_FILES_DIR" -name "$test_file_name" -type f 2>/dev/null)
file_count=$(echo "$found_files" | grep -v "^$" | wc -l)

if [ "$file_count" -eq 0 ]; then
  echo "ERROR: Test file $test_file_name not found in $TEST_FILES_DIR or its subdirectories."
  exit 1
elif [ "$file_count" -gt 1 ]; then
  echo "WARNING: Multiple test files found with name $test_file_name:"
  echo "$found_files"
  echo "Please select one file by using its full path or make the suite name more specific."
  exit 1
else
  test_file="$found_files"
fi

# Find the test output file recursively in TEST_FILES_DIR
found_files=$(find "$TEST_FILES_DIR" -name "$test_output_name" -type f 2>/dev/null)
file_count=$(echo "$found_files" | grep -v "^$" | wc -l)

if [ "$file_count" -eq 0 ]; then
  output_file=""
elif [ "$file_count" -gt 1 ]; then
  echo "WARNING: Multiple test output files found with name $test_output_name:"
  echo "$found_files"
  echo "Please select one file by using its full path or make the suite name more specific."
  exit 1
else
  output_file="$found_files"
fi

echo "Using configuration:"
echo "  DORIS_HOME    = $DORIS_HOME"
echo "  Suite name    = $suite_name"
echo "  Test file     = $test_file"
echo "  Output file   = $output_file"
echo "  Target dir    = $TARGET_DIR"
echo "  Output dir    = $OUTPUT_DIR"

# If target directory already exists, delete it
if [ -d "$TARGET_DIR" ]; then
  rm -rf "$TARGET_DIR"
fi

if [ -d "$OUTPUT_DIR" ]; then
  rm -rf "$OUTPUT_DIR"
fi

# Create target directory
mkdir -p "$TARGET_DIR"
# Create output directory
mkdir -p "$OUTPUT_DIR"

# Check if directory creation was successful
if [ $? -ne 0 ]; then
  echo "ERROR: Failed to create directory $TARGET_DIR. Please check permissions."
  exit 1
fi

# Copy test file to target directory
cp "$test_file" "$TARGET_DIR"

# Check if copy was successful
if [ $? -ne 0 ]; then
  echo "ERROR: Failed to copy test file. Please check path and permissions."
  rm -rf "$TARGET_DIR"
  exit 1
fi

# Copy output file to output directory if exists
if [ "$file_count" -ne 0  ]; then
  cp "$output_file" "$OUTPUT_DIR"
  # Check if copy was successful
  if [ $? -ne 0 ]; then
    echo "ERROR: Failed to copy output file. Please check path and permissions."
    rm -rf "$OUTPUT_DIR"
    exit 1
  fi
fi

# Run regression-test.sh script directly with the suite name
sh "$DORIS_HOME/run-regression-test.sh" --run "$suite_name"

# Check if run was successful
if [ $? -ne 0 ]; then
  echo "ERROR: Failed to run regression-test.sh script."
  # If run failed, delete target directory
  rm -rf "$OUTPUT_DIR"
  rm -rf "$TARGET_DIR"
  exit 1
fi

rm -rf "$OUTPUT_DIR"
rm -rf "$TARGET_DIR"

if [ $? -ne 0 ]; then
  echo "WARNING: Failed to delete directory $TARGET_DIR. Please check permissions."
  exit 1
fi

echo "SUCCESS: Script execution completed. Test suite has been run and cleanup is complete."