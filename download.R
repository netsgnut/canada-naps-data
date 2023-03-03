source("config.R")

globalVariables(c("years", "pollutants"))

for (year in years) {
  for (pollutant in pollutants) {
    # The file is not up yet, and will be available around March 2023
    # Before that, we use the file they provided via email
    if (year == 2013 && pollutant == "NOX") {
      next
    }

    filename <- paste0(pollutant, "_", year, ".csv")
    url <- paste0("https://data-donnees.ec.gc.ca/data/air/monitor/",
                  "national-air-pollution-surveillance-naps-program/",
                  "Data-Donnees/", year, "/",
                  "ContinuousData-DonneesContinu/HourlyData-DonneesHoraires/",
                  filename)
    destfile <- paste0("data/raw/", filename)
    download.file(url, destfile)
  }
}
