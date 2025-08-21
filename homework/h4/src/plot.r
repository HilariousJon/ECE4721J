require(ggplot2)

df <- read.csv("./data/2024_temperature_data.csv")

p1 <- ggplot(data = df, aes(x = factor(month), y = temperature / 10, color = continent)) +
    geom_line(aes(group = continent)) +
    geom_point() +
    xlab("Month") +
    ylab("Average Temperature (degree celsius)")

ggsave("../img/monthly_temp_variation.jpg", plot = p1, width = 8, height = 5)

p2 <- ggplot(df, aes(continent, temperature / 10, fill = continent)) +
    geom_bar(position = "dodge", stat = "summary", fun = "mean") +
    xlab("Continent") +
    ylab("Average Temperature (degree celsius)") +

ggsave("../img/avg_temp_by_continent.jpg", plot = p2, width = 8, height = 5)
