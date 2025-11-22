"""
Python Commenting Guide - Complete Examples
==========================================

This file demonstrates all the different ways to add comments to Python code,
following best practices and PEP 8 style guidelines.

Author: EDA LLM Assistant
Date: September 30, 2025
"""

# ============================================================================
# 1. SINGLE LINE COMMENTS
# ============================================================================

# This is a basic single line comment
x = 5  # This is an inline comment explaining the variable

# Use comments to explain WHY, not WHAT
# Bad: x = x + 1  # Increment x by 1
# Good: x = x + 1  # Account for zero-based indexing

# Comments should be complete sentences with proper capitalization
# and punctuation when possible


# ============================================================================
# 2. MULTI-LINE COMMENTS
# ============================================================================

"""
This is a multi-line string that can serve as a multi-line comment.
It's actually a docstring when placed at the top of a module, class, or function.

Use triple quotes for longer explanations:
- Multiple bullet points
- Complex algorithm explanations  
- Temporary code blocks you want to disable
"""

# Alternative multi-line comment style:
# This approach uses multiple single-line comments
# Each line starts with a hash symbol
# Some developers prefer this style for consistency


# ============================================================================
# 3. DOCSTRINGS (Special Comments for Documentation)
# ============================================================================

def calculate_statistics(data):
    """
    Calculate basic statistics for a dataset.
    
    This function computes mean, median, and standard deviation
    for numerical data in a pandas DataFrame or list.
    
    Args:
        data (list or pd.DataFrame): Input data for analysis
        
    Returns:
        dict: Dictionary containing statistical measures
        
    Raises:
        ValueError: If data is empty or contains non-numeric values
        
    Example:
        >>> data = [1, 2, 3, 4, 5]
        >>> stats = calculate_statistics(data)
        >>> print(stats['mean'])
        3.0
    """
    import statistics
    
    # Input validation
    if not data:
        raise ValueError("Data cannot be empty")
    
    # Calculate statistics
    result = {
        'mean': statistics.mean(data),
        'median': statistics.median(data),
        'stdev': statistics.stdev(data) if len(data) > 1 else 0
    }
    
    return result


class DataProcessor:
    """
    A class for processing and analyzing data.
    
    This class provides methods for loading, cleaning, and analyzing
    various types of data files commonly used in data science projects.
    
    Attributes:
        file_path (str): Path to the data file
        data (pd.DataFrame): Loaded data
        processed (bool): Whether data has been processed
    """
    
    def __init__(self, file_path):
        """
        Initialize the DataProcessor.
        
        Args:
            file_path (str): Path to the data file to process
        """
        self.file_path = file_path
        self.data = None
        self.processed = False


# ============================================================================
# 4. COMMENT STYLES AND BEST PRACTICES
# ============================================================================

# Section headers using equals signs
# ====================================

# Subsection headers using dashes
# --------------------------------

# TODO: Implement advanced statistical functions
# FIXME: Handle edge case when data contains NaN values
# NOTE: This algorithm assumes data is normally distributed
# WARNING: This function modifies the original data

# Code organization comments
def main():
    # Step 1: Load and validate data
    data = load_data()
    
    # Step 2: Clean and preprocess
    cleaned_data = clean_data(data)
    
    # Step 3: Perform analysis
    results = analyze_data(cleaned_data)
    
    # Step 4: Generate report
    generate_report(results)


# ============================================================================
# 5. COMMENTING FOR DIFFERENT SCENARIOS
# ============================================================================

# Algorithm explanation
def bubble_sort(arr):
    """Sort array using bubble sort algorithm."""
    n = len(arr)
    
    # Traverse through all array elements
    for i in range(n):
        # Last i elements are already in place
        for j in range(0, n - i - 1):
            # Traverse the array from 0 to n-i-1
            # Swap if the element found is greater than the next element
            if arr[j] > arr[j + 1]:
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
    
    return arr


# Complex business logic
def calculate_pricing(base_price, customer_type, quantity, season):
    """Calculate final price with various discounts and adjustments."""
    
    # Base calculations
    subtotal = base_price * quantity
    
    # Apply customer type discount
    if customer_type == 'premium':
        discount = 0.15  # 15% discount for premium customers
    elif customer_type == 'regular':
        discount = 0.05  # 5% discount for regular customers
    else:
        discount = 0.0   # No discount for new customers
    
    # Seasonal adjustments
    seasonal_multiplier = {
        'winter': 0.9,   # 10% off in winter
        'spring': 1.0,   # No adjustment
        'summer': 1.1,   # 10% premium in summer
        'fall': 0.95     # 5% off in fall
    }.get(season, 1.0)
    
    # Calculate final price
    discounted_price = subtotal * (1 - discount)
    final_price = discounted_price * seasonal_multiplier
    
    return final_price


# ============================================================================
# 6. COMMENTING OUT CODE (Temporary Disabling)
# ============================================================================

def experimental_function():
    """Function with some experimental code."""
    
    # Current working implementation
    result = simple_calculation()
    
    # Temporarily disabled - testing new approach
    # result = complex_calculation()
    # result = apply_advanced_filter(result)
    
    """
    Alternative implementation - disabled for now
    
    if use_advanced_mode:
        result = advanced_calculation()
        result = post_process(result)
    else:
        result = simple_calculation()
    """
    
    return result


# ============================================================================
# 7. INLINE COMMENTS - WHEN AND HOW TO USE
# ============================================================================

def process_data(data):
    """Process data with inline comments showing good practices."""
    
    # Good inline comments - explain WHY or provide context
    threshold = 0.95  # Confidence threshold based on statistical analysis
    max_retries = 3   # Network timeout protection
    
    # Avoid obvious inline comments
    # Bad: count = 0  # Initialize count to zero
    # Good: count = 0  # Track number of valid records processed
    count = 0  # Track number of valid records processed
    
    for item in data:
        if item.confidence > threshold:  # Only process high-confidence items
            process_item(item)
            count += 1
        
        # Handle special cases
        if count >= 1000:  # Prevent memory overflow with large datasets
            flush_buffer()
            count = 0
    
    return count


# ============================================================================
# 8. COMMENTS FOR DEBUGGING AND DEVELOPMENT
# ============================================================================

def debug_example():
    """Example of debugging comments."""
    
    data = load_data()
    print(f"Debug: Loaded {len(data)} records")  # Temporary debug output
    
    # Debug: Print first few records to verify format
    # for i, record in enumerate(data[:3]):
    #     print(f"Record {i}: {record}")
    
    # Performance monitoring
    import time
    start_time = time.time()
    
    result = expensive_operation(data)
    
    # Debug: Monitor performance
    elapsed = time.time() - start_time
    print(f"Debug: Operation took {elapsed:.2f} seconds")
    
    return result


# ============================================================================
# 9. COMMENTS FOR CODE MAINTENANCE
# ============================================================================

# Version history comments
"""
Version 2.1.0 - Added support for JSON files
Version 2.0.0 - Major refactor, breaking changes
Version 1.5.0 - Added error handling and logging
Version 1.0.0 - Initial release
"""

# Dependency notes
# Requires: pandas >= 1.3.0, numpy >= 1.20.0
# Optional: matplotlib for visualization features

# Known issues and limitations
"""
KNOWN ISSUES:
- Memory usage can be high with files > 1GB
- Unicode support limited in CSV files
- Performance degrades with > 10M records

TODO:
- Add support for streaming large files
- Implement parallel processing
- Add data validation hooks
"""


# ============================================================================
# 10. COMMENT FORMATTING AND STYLE
# ============================================================================

#Bad: no space after hash
# Good: proper spacing after hash

# Use consistent indentation in multi-line comments
def example_function():
    # This comment is properly aligned
    # with the code block it describes
    # and maintains consistent indentation
    pass

    # Separate logical sections with blank lines
    
    # This is another section
    # with its own comments


if __name__ == "__main__":
    print("Python Commenting Guide")
    print("See the code above for comprehensive examples!")