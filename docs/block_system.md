# Investment Portfolio Simulation Block System

## Overview
The Block System is a modular architecture designed for building investment portfolio simulations. It allows users to construct complex investment strategies using interconnected blocks, similar to a building block system. Each block represents a specific function or decision in the investment strategy.

## Block Types

### 1. Asset Block
Represents an individual tradable asset (stock).
```json
{
  "blocktype": "Asset",
  "attributes": {
    "ticker": "TSLA",
    "company_name": "Tesla Inc.",
    "exchange": "Nasdaq"
  }
}
```
- No children allowed
- Basic validation of ticker symbols
- Future extension: Additional metadata (currency, asset class, trading status)

### 2. Group Block
Organizes other blocks into logical units. Always contains a Weight Block as its first child.
```json
{
  "blocktype": "Group",
  "attributes": {
    "name": "Tech Stocks"
  },
  "children": [
    {
      "blocktype": "Weight",
      "attributes": {
        "weight_type": "equal"
      },
      "children": [/* other blocks */]
    }
    /* additional blocks if needed */
  ]
}
```
- **IMPORTANT**: Automatically includes a Weight Block as its first child
- The Weight Block manages the distribution among the group's elements
- Can contain any other block types after the Weight Block
- Used for logical grouping and organization
- If no specific weighting is provided, the Weight Block defaults to equal distribution

### 3. Weight Block
Defines allocation strategies for funds across children blocks.
```json
{
  "blocktype": "Weight",
  "attributes": {
    "weight_type": {
      "type": "specified",
      "allocation_type": "percentage",
      "values": [40, 30, 30]
    }
  },
  "children": [/* must match number of values */]
}
```

Types of Weighting:
- `equal`: Distributes equally among children
- `specified`: Custom allocation with array of values (must sum to 100%)
- `inverse_volatility`: Weights based on inverse of volatility
- `market_cap`: Weights based on market capitalization

Key Rule: For specified weights, the number of values must match the number of children exactly.

### 4. Condition Block
Implements if/else logic based on market conditions or metrics.
```json
{
  "blocktype": "Condition",
  "attributes": {
    "condition": {
      "function": {
        "function_name": "current_price",
        "window_of_days": null,
        "asset": "TSLA"
      }
    },
    "operator": ">",
    "compare_to": {
      "type": "fixed_value",
      "value": 100,
      "unit": "$"
    }
  },
  "children": [/* exactly 2 blocks: if-true and if-false */]
}
```
- Requires exactly two children (if-true and if-false blocks)
- Optional window_of_days parameter depending on function
- Supports comparison between functions or fixed values

### 5. Filter Block
Selects assets based on sorting criteria.
```json
{
  "blocktype": "Filter",
  "attributes": {
    "sort_function": {
      "function_name": "cumulative_return",
      "window_of_days": 2
    },
    "select": {
      "option": "Top",
      "amount": 3
    }
  },
  "children": [/* only Asset blocks allowed */]
}
```
- Only accepts Asset blocks as children
- If amount exceeds available assets, returns all available assets
- No minimum number of children required

## Nesting Rules
- Group blocks must have a Weight Block as their first child
- All blocks can be nested within each other EXCEPT:
  - Asset blocks cannot have children
  - Filter blocks can only have Asset blocks as children
  - Condition blocks must have exactly two children

## Validation Rules
1. Group Block:
   - First child MUST be a Weight Block
   - Additional children can be any block type

2. Weight Block (Specified):
   - Values array must sum to 100%
   - Number of values must match number of children
   
3. Condition Block:
   - Must have exactly two children
   - window_of_days is optional based on function

4. Filter Block:
   - Only Asset blocks allowed as children
   - amount can exceed available assets (will return all available)

## Functions
Functions are defined as enums rather than strings for type safety. Available functions include:
- current_price
- cumulative_return
(Additional functions to be added based on requirements)

## Error Handling
- Invalid block structure
- Incorrect children types
- Invalid weight distributions
- Missing required attributes
- Missing Weight Block in Group Block
- Future: Non-tradeable assets

## Example Complex Structure
```json
{
  "blocktype": "Group",
  "attributes": {
    "name": "Portfolio Strategy"
  },
  "children": [
    {
      "blocktype": "Weight",
      "attributes": {
        "weight_type": "equal"
      },
      "children": [
        {
          "blocktype": "Filter",
          "attributes": {
            "sort_function": {
              "function_name": "cumulative_return",
              "window_of_days": 30
            },
            "select": {
              "option": "Top",
              "amount": 5
            }
          },
          "children": [/* Asset blocks */]
        },
        {
          "blocktype": "Group",
          "attributes": {
            "name": "Conditional Investment"
          },
          "children": [
            {
              "blocktype": "Weight",
              "attributes": {
                "weight_type": "specified",
                "values": [60, 40]
              },
              "children": [/* Additional blocks */]
            }
          ]
        }
      ]
    }
  ]
}

WEIGHTS Appearances:
// Equal weighting
{
  "blocktype": "Weight",
  "attributes": {

      "type": "equal"
    
  }
}

// Specified weighting
{
  "blocktype": "Weight",
  "attributes": {

      "type": "specified",
      "allocation_type": "percentage",
      "values": [30, 30, 30, 10]
    }
  
}

// Inverse volatility weighting
{
  "blocktype": "Weight",
  "attributes": {

      "type": "inverse_volatility",
      "window_of_trading_days": 20
    
  }
}

// Market cap weighting
{
  "blocktype": "Weight",
  "attributes": {
    "weight_type": {
      "type": "market_cap"
    }
  }
}
```

## Future Considerations
1. Asset metadata expansion
   - Currency
   - Asset class
   - Trading status
2. Advanced validation for non-tradeable assets
3. Additional function types
4. Performance optimization for deep nested structures
5. Extended Weight Block configuration options