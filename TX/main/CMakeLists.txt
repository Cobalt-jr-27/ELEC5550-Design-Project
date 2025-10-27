idf_component_register(SRCS "TX.c"
                       INCLUDE_DIRS "."
                       PRIV_REQUIRES usb esp_driver_gpio driver esp_timer
                       )


