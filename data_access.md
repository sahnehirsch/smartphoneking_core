# Data Access Documentation for data_for_api Table

## Table Overview
The `data_for_api` table stores smartphone pricing and variant information from different retailers. It includes details about smartphone models, their variants, pricing, and hotness metrics.

## Table Structure

| Column Name    | Data Type | Description | Constraints |
|---------------|-----------|-------------|-------------|
| price_id      | bigint    | Unique identifier for each price entry | PRIMARY KEY |
| smartphone_id | integer   | Reference ID for the smartphone | NOT NULL |
| retailer_id   | integer   | Reference ID for the retailer | NOT NULL |
| retailer_name | text      | Name of the retailer | NOT NULL |
| price         | numeric   | Current price of the smartphone | NOT NULL |
| product_url   | text      | URL to the product page on retailer's website | Optional |
| is_hot        | boolean   | Flag indicating if the price is considered "hot" | Default: false |
| hotness_score | integer   | Numerical score indicating how good the deal is | Default: 0 |
| oem           | text      | Manufacturer of the smartphone | NOT NULL |
| model         | text      | Model name of the smartphone | NOT NULL |
| color_variant | text      | Color variant of the smartphone | Optional |
| ram_variant   | text      | RAM specification of the variant | Optional |
| rom_variant   | text      | Storage specification of the variant | Optional |
| variant_rank  | integer   | Ranking of the variant within the model lineup | Optional |
| os            | text      | Operating system of the smartphone | Optional |
| run_id        | uuid      | Identifier for the data collection run | NOT NULL |

## Accessing the Data

### Authentication
To access the data from your front-end application, you'll need:
1. Your Supabase project URL
2. Your Supabase anon/public key

These credentials should be stored securely in your application's environment variables.

### Setting Up Supabase Client
```javascript
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY

const supabase = createClient(supabaseUrl, supabaseAnonKey)
```

### Common Query Examples

#### Fetch All Active Hot Deals
```javascript
const { data, error } = await supabase
  .from('data_for_api')
  .select('*')
  .eq('is_hot', true)
  .order('hotness_score', { ascending: false })
```

#### Get Prices for Specific Model
```javascript
const { data, error } = await supabase
  .from('data_for_api')
  .select('*')
  .eq('model', 'iPhone 15 Pro')
  .order('price', { ascending: true })
```

#### Get Variants of a Model
```javascript
const { data, error } = await supabase
  .from('data_for_api')
  .select('*')
  .eq('model', 'Galaxy S23')
  .order('variant_rank', { ascending: true })
```

#### Get Prices by OEM
```javascript
const { data, error } = await supabase
  .from('data_for_api')
  .select('*')
  .eq('oem', 'Samsung')
  .order('price', { ascending: true })
```

#### Get Prices by Retailer Name
```javascript
const { data, error } = await supabase
  .from('data_for_api')
  .select('*')
  .eq('retailer_name', 'Amazon')
  .order('price', { ascending: true })
```

### Error Handling
Always check for errors when making queries:
```javascript
if (error) {
  console.error('Error:', error.message)
  return
}
// Process your data here
console.log('Data:', data)
```

## Best Practices
1. Always use environment variables for Supabase credentials
2. Implement proper error handling
3. Consider caching frequently accessed data to reduce database load
4. Use the appropriate indexes for your queries (price_id, smartphone_id, and retailer_id are good candidates)
5. When querying for hot deals, combine is_hot and hotness_score filters for better results

## Performance Considerations
- The table uses a bigint PRIMARY KEY for price_id which allows for a large number of entries
- Consider using pagination when fetching large datasets
- Use specific column selection instead of '*' when you don't need all fields
- Add appropriate indexes based on your most common query patterns

---
Last Updated: 2025-01-11
