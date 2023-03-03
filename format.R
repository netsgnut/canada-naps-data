source("config.R")

globalVariables(c(years, pollutants))

library(data.table)
library(lubridate)
library(tidyverse)

get_year_raw_df <- function(year) {
    df <- tibble::tibble()

    for (pollutant in pollutants) {
        file_path <- paste0("data/raw/", pollutant, "_", year, ".csv")

        # We have to do this, because some raw files has different format
        skip <- 6
        if (startsWith(readLines(file_path, n = 1), "File generated on")) {
            skip <- skip + 2
        }

        # If for PM25, the columns are different
        col_names <- c(
            "Pollutant", "NAPSID",
            "City", "Territory", "Latitude", "Longitude",
            "Date",
            "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
            "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
            "21", "22", "23", "24"
        )
        col_types <- c(
            "c", "i",
            "c", "c", "d", "d",
            "c",
            "d", "d", "d", "d", "d", "d", "d", "d", "d", "d",
            "d", "d", "d", "d", "d", "d", "d", "d", "d", "d",
            "d", "d", "d", "d"
        )
        if ("PM25" == pollutant) {
            col_names <- c(col_names[1:1], "Method", col_names[2:31])
            col_types <- c(col_types[1:1], "i", col_types[2:31])
        }

        # Print
        current_df <- read_csv(
            file_path,
            skip = skip,
            col_names = col_names,
            col_types = col_types
        ) |>
            mutate(
                NAPSID = as.integer(NAPSID),
                Date = case_when(
                    str_detect(Date, "/") ~ mdy(Date),
                    TRUE ~ ymd(Date)
                )
            ) |>
            select(-contains("Method"))

        df <- rbind(df, current_df)
    }

    df
}

# Make `tidy`-ed hourly CSVs
for (year in years) {
    hourly_csv_file <- paste0("data/build/CA_NAPS_Hourly_", year, ".csv")

    hourly_df <- get_year_raw_df(year) |>
        pivot_longer("01":"24", names_to = "Hour", values_to = "Value") |>
        filter(Value >= 0) |>
        select(c("Pollutant", "NAPSID", "City", "Territory",
                 "Latitude", "Longitude", "Date", "Hour", "Value")) |>
        drop_na()

    fwrite(hourly_df |> mutate(Value = round(Value, 2)), hourly_csv_file)
}

# ...and merge them into daily CSVs
for (year in years) {
    hourly_csv_file <- paste0("data/build/CA_NAPS_Hourly_", year, ".csv")
    daily_csv_file <- paste0("data/build/CA_NAPS_Daily_", year, ".csv")

    daily_df <- read_csv(hourly_csv_file) |>
        group_by(Pollutant, NAPSID, City, Territory,
                 Latitude, Longitude, Date) |>
        summarize(Value = mean(Value))

    fwrite(daily_df |> mutate(Value = round(Value, 2)), daily_csv_file)
}

# ...and aggregate them into year sets
for (year_range_size in c(5, 10, 20)) {
    n_ranges <- length(years) %/% year_range_size
    year_ranges <- split(years,
                         rep(1:n_ranges, each = year_range_size))

    for (year_range in year_ranges) {
        daily_aggregated_csv_file <- paste0("data/build/CA_NAPS_Daily_",
                                            min(year_range), "-",
                                            max(year_range), ".csv")

        df <- tibble::tibble()

        for (year in year_range) {
            daily_csv_file <- paste0("data/build/CA_NAPS_Daily_", year, ".csv")
            df <- rbind(df, read_csv(daily_csv_file))
        }

        fwrite(df, daily_aggregated_csv_file)
    }
}
