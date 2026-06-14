idf_component_register(
    SRCS "main.c"
    INCLUDE_DIRS "."
    EMBED_FILES "web/index.html"
    REQUIRES esp_http_server esp_wifi esp_eth esp_driver_gpio esp_timer nvs_flash espressif__mosquitto espressif__mqtt espressif__cjson
)
