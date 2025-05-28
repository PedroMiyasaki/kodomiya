# KodoMiya Configuration Guide

This directory contains the configuration files for the KodoMiya real estate data pipeline.

## Configuration Structure

The main configuration file `config.yml` contains all the settings required for the three web scrapers:
- Zap Imóveis
- Viva Real
- Chaves na Mão

## How to Update Configurations

### When Website HTML Changes

If a website changes its HTML structure, you can update the configuration without changing any code:

1. Inspect the website's HTML structure using your browser's DevTools
2. Find the new HTML tags, class names, or other selectors
3. Update the corresponding fields in `config.yml`

For example, if Zap Imóveis changes its price element from:

```html
<div class="listing-price"><p>R$ 450.000</p></div>
```

to:

```html
<span class="property-value"><strong>R$ 450.000</strong></span>
```

You would update the config like this:

```yaml
# Before
zap_imoveis:
  # ...
  price:
    tag: "div"
    class_name: "listing-price" 
    selector_method: "find_all"
    additional_selectors: [["p", 0]]
    # ...

# After
zap_imoveis:
  # ...
  price:
    tag: "span"
    class_name: "property-value"
    selector_method: "find_all"
    additional_selectors: [["strong", 0]]
    # ...
```

### Adding a New Website Source

To add a new website source:

1. Add a new section to `config.yml` with the same structure as existing sources
2. Create a new class in `src/pipelines/resources/trading_properties_function_classes.py` 
3. Create a new pipeline file in `src/pipelines/`

## Configuration Options

Each source configuration has the following structure:

```yaml
source_name:
  base_url: URL for the source
  pagination_param: Parameter format for pagination
  property_card:
    html_element: Main HTML tag containing property cards
    html_class: Class name for property cards
  price:
    # Configuration for price extraction
  address:
    # Configuration for address extraction
  size:
    # Configuration for property size extraction
  rooms:
    # Configuration for room count extraction
  bathrooms:
    # Configuration for bathroom count extraction
  parking:
    # Configuration for parking space extraction
  search_lat_long_view_box:
    # Lat/long bounding box for geocoding
```

## Database and Geocoding Configuration

The configuration also includes settings for:

- Database connection
- Geocoding parameters
- Logging

Adjust these settings as needed for your environment. 