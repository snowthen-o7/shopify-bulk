"""
GraphQL query templates for Shopify bulk operations.

Each function returns a complete GraphQL query string ready
to pass to `bulkOperationRunQuery`.
"""

from __future__ import annotations


def products_query(*, include_inventory: bool = False) -> str:
    """Products with variants, images, and optionally inventory levels."""
    inventory_fragment = ""
    if include_inventory:
        inventory_fragment = """
              inventoryItem {
                id
                measurement {
                  weight {
                    value
                    unit
                  }
                }
                inventoryLevels(first: 50) {
                  edges {
                    node {
                      quantities(names: ["available", "on_hand", "committed", "incoming"]) {
                        name
                        quantity
                      }
                      location {
                        id
                        name
                      }
                    }
                  }
                }
              }"""
    else:
        inventory_fragment = """
              inventoryItem {
                measurement {
                  weight {
                    value
                    unit
                  }
                }
              }"""

    return """
{
  products {
    edges {
      node {
        id
        legacyResourceId
        title
        descriptionHtml
        handle
        vendor
        productType
        status
        tags
        createdAt
        updatedAt
        onlineStoreUrl
        publishedAt
        totalInventory
        category {
          fullName
        }
        seo {
          title
          description
        }
        featuredImage {
          url
        }
        images(first: 250) {
          edges {
            node {
              url
              altText
            }
          }
        }
        variants(first: 100) {
          edges {
            node {
              id
              legacyResourceId
              title
              sku
              price
              compareAtPrice
              barcode
              inventoryQuantity
              position
              taxable
              availableForSale
              inventoryPolicy
              createdAt
              updatedAt
              selectedOptions {
                name
                value
              }
              image {
                url
                altText
              }""" + inventory_fragment + """
            }
          }
        }
      }
    }
  }
}
"""
