# Investment Portfolio Block System Documentation

## Core Structure
Every block must have:
- `blocktype` as first key (mandatory)
- Additional attributes (block-specific)
- `children` array where allowed

## Block Types & Rules

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
   
2. `"type": "specified"`
   - Requires:
     - `allocation_type`: "percentage" or "fraction"
     - `values`: Array of allocations
   - Number of values must match number of children
   
3. `"type": "inverse_volatility"`
   - Requires: `window_of_trading_days`
   
4. `"type": "market_cap"`
   - No additional attributes needed

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
    "value": 100
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
  - For all other functions: window_of_days required
  - asset is always required
- compare_to can be:
  - fixed_value with a specified value
  - function with same rules as above

### 5. Filter Block
```json
{
  "blocktype": "Filter",
  "sort_function": {
    "function_name": "cumulative_return",
    "window_of_days": 10
  },
  "select": {
    "option": "Top",
    "amount": 3
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

2. Weight Block:
   - type must be one of: ["equal", "specified", "inverse_volatility", "market_cap"]
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

Would you like me to add more details to any section or provide specific examples for certain scenarios?

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