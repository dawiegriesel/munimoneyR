# Municipal Money R Package

# Load Packages
library(httr)
library(dplyr)
library(jsonlite)
library(purrr)
library(glue)
library(curl)

# General Cube Extract Function
get_cube_facts <- purrr::as_mapper(~ httr::GET(
  url = glue("https://municipaldata.treasury.gov.za/api/cubes/{.x}/facts")) %>%
    jsonlite::parse_json(simplifyVector = TRUE))

# Get Table of Municipalities
get_cube_municipalities <- function() {
  x <- get_cube_facts("municipalities")

  return(x$data)
}
munis <- get_cube_municipalities()
# Get Table of Municipal Audit Reports
get_cube_audit_opinions <- function() {
  x <- get_cube_facts("audit_opinions")

  return(x$data)
}

# Get Table of Aggregated Numbers for Financial Performance, Financial Position and Cash Flow
get_cube_aggregate <- function(financial_year_end.year, demarcation.code, amount_type.code, cube) {
  endpoint <- 'https://municipaldata.treasury.gov.za/api/cubes/'
  #cube <- cube
  drilldown <-
    'amount_type.label|amount_type.code|financial_period.period|period_length.length|financial_year_end.year|item.composition|item.return_form_structure|item.position_in_return_form|item.label|item.code|demarcation.label|demarcation.code'

  cut <-
    glue(
      'financial_year_end.year:{financial_year_end.year}|amount_type.code:{amount_type.code}|financial_period.period:{financial_year_end.year}|demarcation.code:{demarcation.code}'
    )

  req <-
    curl::curl_fetch_memory(
      glue(
        '{endpoint}{cube}/aggregate?drilldown={drilldown}&cut={cut}&aggregates=amount.sum&order=item.position_in_return_form:asc&format=json'
      )
    )

  #parse_headers(req$headers)
  x <- jsonlite::fromJSON(rawToChar(req$content))
  x <- x$cells
  #tibble(x) %>% mutate(cube.code = paste0(cube))
  return(x)
}

get_cube_aggregate(2018, "TSH", "AUDA", "cflow") %>%
  filter(item.code == 4200)

# Get single value for a line item (item.code) for a Municipality for a specific year and amount_type.code
get_cube_aggregate_item <- function(financial_year_end.year, demarcation.code, amount_type.code, cube, item.code) {
  endpoint <- 'https://municipaldata.treasury.gov.za/api/cubes/'
  #cube <- cube
  drilldown <-
    'amount_type.label|amount_type.code|financial_period.period|period_length.length|financial_year_end.year|item.composition|item.return_form_structure|item.position_in_return_form|item.label|item.code|demarcation.label|demarcation.code'

  cut <-
    glue(
      'financial_year_end.year:{financial_year_end.year}|amount_type.code:{amount_type.code}|financial_period.period:{financial_year_end.year}|demarcation.code:{demarcation.code}|item.code:{item.code}'
    )

  req <-
    curl::curl_fetch_memory(
      glue(
        '{endpoint}{cube}/aggregate?drilldown={drilldown}&cut={cut}&aggregates=amount.sum&order=item.position_in_return_form:asc&format=json'
      )
    )

  #parse_headers(req$headers)
  x <- jsonlite::fromJSON(rawToChar(req$content))
  x <- x$cells
  #tibble(x) %>% mutate(cube.code = paste0(cube))
  return(x)
}

get_cube_aggregate_item(2018, "TSH", "AUDA", "cflow", "4200")

x <- curl::curl_fetch_memory('https://municipaldata.treasury.gov.za/api/cubes/incexp/aggregate?drilldown=demarcation.code|demarcation.label|item.code|item.label|financial_year_end.year&cut=financial_year_end.year:2015|amount_type.code:AUDA|financial_period.period:2017|demarcation.code:"TSH"|item.code:"2800";"5200"&aggregates=amount.sum')
x <- jsonlite::fromJSON(rawToChar(x$content))
x$cells
