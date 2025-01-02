# Investment Portfolio Block System Documentation

## Core Structure
Every block must have:
- `blocktype` as first key (mandatory)
- Additional attributes (block-specific)
- `children` array where allowed

## Block Types & Rules

## Block Format Reference - Quickview

{
  "Group Block": {
    "blocktype": "Group",
    "name": "string (required)",
    "children": ["at least one child, first must be Weight"]
  },
  
  "Weight Block": {
    "blocktype": "Weight",
    "type": "enum: equal | specified | inverse_volatility | market_cap",
    "allocation_type": "enum: percentage | fraction (required for specified)",
    "values": ["array of numbers (required for specified)"],
    "window_of_trading_days": "number (required for inverse_volatility)",
    "children": ["must match values array length for specified"]
  },
  
  "Condition Block": {
    "blocktype": "Condition",
    "function": {
      "function_name": "enum (see FunctionName)",
      "window_of_days": "number (required for most functions)",
      "asset": "string (required)"
    },
    "operator": "enum: > | < | = | >= | <=",
    "compare_to": {
      "type": "fixed_value | function",
      "value": "number (for fixed_value)",
      "unit": "string (optional for fixed_value)",
      "function": "FunctionDefinition (for function type)"
    },
    "children": ["exactly 2 children required"]
  },
  
  "Filter Block": {
    "blocktype": "Filter",
    "sort_function": {
      "function_name": "enum (see FunctionName)",
      "window_of_days": "number (required)"
    },
    "select": {
      "option": "enum: Top | Bottom",
      "amount": "number"
    },
    "children": ["array of Asset blocks only"]
  },
  
  "Asset Block": {
    "blocktype": "Asset",
    "ticker": "string (required)",
    "company_name": "string (required)",
    "exchange": "string (required)",
    "children": "not allowed"
  }
}



### 1. Group Block
```json
{
  "blocktype": "Group",
  "name": "Strategy Name",
  "children": [
    // First child is always Weight Block
    {
      "blocktype": "Weight",
      "type": "equal",
      "children": []
    },
    // Additional blocks...
  ]
}
```

**Rules:**
- First child is ALWAYS a Weight Block (automatically added)
- Default Weight Block has `type: "equal"`
- Can contain any other blocks after Weight Block
- Name must be a non-empty string

### 2. Weight Block
```json
{
  "blocktype": "Weight",
  "type": "specified",
  "allocation_type": "percentage",  // Optional, only for type "specified"
  "values": [40, 30, 30],          // Optional, only for type "specified"
  "window_of_trading_days": 20,     // Optional, only for type "inverse_volatility"
  "children": []
}
```

**Type Options & Requirements:**
1. `"type": "equal"`
   - No additional attributes needed
   - Automatically assigns equal weights to all children
   
2. `"type": "specified"`
   - Requires:
     - `allocation_type`: "percentage" or "fraction"
     - `values`: Array of allocations
   - Number of values must match number of children
   - For "percentage": values must sum to 100
   - For "fraction": values must sum to 1
   
3. `"type": "inverse_volatility"`
   - Requires: `window_of_trading_days` (positive integer)
   - Weights inversely proportional to asset volatility

### 3. Asset Block
```json
{
  "blocktype": "Asset",
  "ticker": "TSLA",
  "company_name": "Tesla Inc.",
  "exchange": "NASDAQ"
}
```

**Rules:**
- No children allowed
- All fields are mandatory
- All fields must be non-empty strings
- Exchange must be a valid exchange identifier (e.g., "NYSE", "NASDAQ")

### 4. Condition Block
```json
{
  "blocktype": "Condition",
  "function": {
    "function_name": "cumulative_return",
    "window_of_days": 10,          // Optional, not needed for "current_price"
    "asset": "TSLA"
  },
  "operator": ">",
  "compare_to": {
    "type": "fixed_value",
    "value": 100,
    "unit": "$"                    // Optional, can be "$" or "%"
  },
  // OR
  "compare_to": {
    "type": "function",
    "function": {
      "function_name": "current_price",
      "window_of_days": null,      // Optional, not needed for "current_price"
      "asset": "QQQ"
    }
  },
  "children": [
    // Exactly 2 children required
  ]
}
```

**Rules:**
- Must have exactly 2 children:
  1. First child: executed if condition is true
  2. Second child: executed if condition is false
- Function rules:
  - For function_name "current_price": no window_of_days needed
  - For all other functions: window_of_days required (positive integer)
  - asset is always required
- Operators must be one of: ">", "<", "=", ">="
- compare_to can be:
  - fixed_value with a specified value
  - function with same rules as above
- if fixed value is selected:
  - unit is optional
  - unit can be "$" or "%" depending on the function selected

### 5. Filter Block
```json
{
  "blocktype": "Filter",
  "sort_function": {
    "function_name": "cumulative_return",
    "window_of_days": 10           // Required for all functions except current_price
  },
  "select": {
    "option": "Top",               // Must be "Top" or "Bottom"
    "amount": 3                    // Must be positive integer
  },
  "children": [
    // Only Asset blocks allowed
  ]
}
```

**Rules:**
- Can only contain Asset blocks as children
- If selected amount exceeds available assets, returns all available
- No minimum number of children required
- sort_function requires both function_name and window_of_days
- select.option must be either "Top" or "Bottom"
- select.amount must be a positive integer

## Available Functions

1. `current_price`:
   - No window_of_days needed
   - Returns current price value in $
   - Only requires asset

2. `cumulative_return`:
   - Requires window_of_days (max: 252 days)
   - Returns value in %
   - Requires asset

3. `simple_moving_average`:
   - Requires window_of_days (max: 252 days)
   - Returns moving average price in $
   - Requires asset

4. `exponential_moving_average`:
   - Requires window_of_days (max: 500 days)
   - Returns moving average price in $
   - Requires asset

5. `moving_average_of_returns`:   - Requires window_of_days (max: 252 days)
   - Returns average of returns in %
   - Requires asset

6. `relative_strength_index`:
   - Requires window_of_days (max: 252 days)
   - Returns RSI value in range [0, 100]
   - Requires asset

7. `price_standard_deviation`:
   - Requires window_of_days (max: 252 days)
   - Returns standard deviation of prices in $
   - Requires asset

8. `returns_standard_deviation`:
   - Requires window_of_days (max: 252 days)
   - Returns standard deviation of returns in %
   - Requires asset

9. `max_drawdown`:
    - Requires window_of_days (max: 252 days)
    - Returns maximum drawdown in %
    - Requires asset

## Function Rules
1. `current_price`:
   - No window_of_days needed
   - Only requires asset

2. All other functions:
   - Require window_of_days
   - Window must be positive integer
   - Window cannot exceed max limit (500 for EMA, 252 for others)
   - Require asset

## Validation Rules
1. Group Block:
   - First child must be Weight Block
   - Weight Block must have valid type
   - Name must be non-empty string

2. Weight Block:
   - type must be one of: ["equal", "specified", "inverse_volatility", "market_cap"]
   - For "specified":
     - values array must have same length as children
     - allocation_type must be "percentage" or "fraction"
     - values must sum to 100 for "percentage"
     - values must sum to 1 for "fraction"
   - For "inverse_volatility":
     - window_of_trading_days must be positive integer

3. Condition Block:
   - Exactly 2 children
   - Valid function configuration
   - Valid operator (">", "<", "=", ">=")
   - Valid compare_to configuration
   - window_of_days must be positive integer when required

4. Filter Block:
   - Only Asset blocks as children
   - Valid sort_function
   - Valid select criteria
   - amount must be positive integer
   - option must be "Top" or "Bottom"

## Function Rules
1. `current_price`:
   - No window_of_days needed
   - Only requires asset

2. All other functions (8 total):
   - Require window_of_days
   - Require asset

## Validation Rules
1. Group Block:
   - First child must be Weight Block
   - Weight Block must have valid type
   - Name must be a non-empty string

2. Weight Block:
   - type must be one of: ["equal", "specified", "inverse_volatility"]
   - For "specified":
     - values array must have same length as children
     - allocation_type must be "percentage" or "fraction"
   - For "inverse_volatility":
     - window_of_trading_days required

3. Condition Block:
   - Exactly 2 children
   - Valid function configuration
   - Valid operator
   - Valid compare_to configuration

4. Filter Block:
   - Only Asset blocks as children
   - Valid sort_function
   - Valid select criteria


## Error Handling

### Common Errors
- Insufficient data for calculation
- Invalid date range
- Invalid period (must be positive)
- Invalid ticker
- Invalid function parameters
- Invalid weight allocation
- Invalid filter configuration

### Error Codes
- DatabaseError::InsufficientData
- DatabaseError::InvalidDateRange
- DatabaseError::InvalidPeriod
- DatabaseError::InvalidInput

MERMAID DIAGRAM:

classDiagram
    class Block {
        +blocktype: string
        +children: Block[]
    }

    class GroupBlock {
        +blocktype: "Group"
        +name: string
        +children: Block[]
        Note: First child always WeightBlock
    }

    class WeightBlock {
        +blocktype: "Weight"
        +type: string
        +allocation_type?: string
        +values?: number[]
        +window_of_trading_days?: number
        +children: Block[]
    }

    class AssetBlock {
        +blocktype: "Asset"
        +ticker: string
        +company_name: string
        +exchange: string
        Note: No children allowed
    }

    class ConditionBlock {
        +blocktype: "Condition"
        +function: Function
        +operator: string
        +compare_to: CompareValue
        +children[2]: Block[]
        Note: Exactly 2 children
    }

    class FilterBlock {
        +blocktype: "Filter"
        +sort_function: Function
        +select: SelectCriteria
        +children: AssetBlock[]
        Note: Only Asset children
    }

    class Function {
        +function_name: string
        +window_of_days?: number
        +asset: string
    }

    class WeightType {
        <<enumeration>>
        equal
        specified
        inverse_volatility
        market_cap
    }

    Block <|-- GroupBlock
    Block <|-- WeightBlock
    Block <|-- AssetBlock
    Block <|-- ConditionBlock
    Block <|-- FilterBlock
    
    GroupBlock --> WeightBlock : first child
    WeightBlock --> WeightType : type
    ConditionBlock --> Function
    FilterBlock --> Function
    FilterBlock --> AssetBlock
    
    note for WeightBlock "Type determines required fields:
    - equal: no additional fields
    - specified: allocation_type & values
    - inverse_volatility: window_of_trading_days
    - market_cap: no additional fields"
    
    note for Function "current_price: no window_of_days needed
    all other functions: window_of_days required"