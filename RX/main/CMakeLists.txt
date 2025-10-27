idf_component_register(
    SRCS "RX.c"
    INCLUDE_DIRS "."
    REQUIRES driver esp_timer tinyusb
)
