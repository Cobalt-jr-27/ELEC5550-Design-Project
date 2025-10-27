// ----------- TX: Manchester/HID over GPIO with LEDs + ALIGN beacons + ACK RX
// (I2S oversample, WDT-safe, gated TX by current monitor, NO laser enable pin) ------------
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <inttypes.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"

#include "esp_timer.h"
#include "esp_err.h"
#include "esp_log.h"
#include "esp_rom_sys.h"
#include "driver/gpio.h"
#include "driver/i2c.h"

#include "usb/usb_host.h"
#include "usb/hid_host.h"
#include "usb/hid_usage_keyboard.h"

/* ===================== Link rate config  ===================== */
#define DATA_RATE_BPS       60000        // set data rate (bits/s) must match RX.c
#define OS_BITS_PER_DATA    32           // logic samples per data bit

/* Derived timings */
#define BIT_DURATION_US     (1000000 / DATA_RATE_BPS) // MUST be >= 2
#define HALF_BIT_US         (BIT_DURATION_US / 2)
#define FRAME_GAP_BITS      4
#define INTERFRAME_US       (BIT_DURATION_US * FRAME_GAP_BITS)

/* ===================== LEDs ===================== */
#define LED_RED    GPIO_NUM_7   // activity/power
#define LED_GREEN  GPIO_NUM_8   // link/alignment
#define LED_YELLOW GPIO_NUM_9   // error / overcurrent

static inline void led_init(void)
{
    gpio_config_t io = {
        .pin_bit_mask = (1ULL<<LED_RED) | (1ULL<<LED_GREEN) | (1ULL<<LED_YELLOW),
        .mode = GPIO_MODE_OUTPUT,
        .pull_up_en = 0, .pull_down_en = 0, .intr_type = GPIO_INTR_DISABLE
    };
    gpio_config(&io);

    // Power on: RED ON (power/activity), others OFF
    gpio_set_level(LED_RED, 1);
    gpio_set_level(LED_GREEN, 0);
    gpio_set_level(LED_YELLOW, 0);
}
static inline void led_set_align(bool on)  { gpio_set_level(LED_GREEN, on ? 1 : 0); }
static inline void led_error_on(void)      { gpio_set_level(LED_YELLOW, 1); }
static inline void led_error_off(void)     { gpio_set_level(LED_YELLOW, 0); }
static inline void led_tx_on(void)         { gpio_set_level(LED_RED, 1); }
static inline void led_tx_off(void)        { gpio_set_level(LED_RED, 0); }

/* ===================== Laser current monitor / INA219 ===================== */

#define I2C_PORT            I2C_NUM_0
#define I2C_SDA_GPIO        4       // SDA line to INA219
#define I2C_SCL_GPIO        5       // SCL line to INA219
#define I2C_FREQ_HZ         400000

// INA219 registers
#define INA219_ADDR         0x40
#define INA219_REG_CONFIG   0x00
#define INA219_REG_SHUNT    0x01

#define LASER_I_LIMIT_A     0.03f   // 30 mA cutoff
#define LASER_SHUNT_OHMS    0.1f
#define LASER_POLL_HZ       2000     // poll 2000 Hz

static volatile bool g_laser_tripped = false;
static bool g_current_monitor_present = false;


static volatile bool g_tx_allowed = true;

static esp_err_t i2c_master_init_simple(void)
{
    i2c_config_t conf = {
        .mode = I2C_MODE_MASTER,
        .sda_io_num = I2C_SDA_GPIO,
        .scl_io_num = I2C_SCL_GPIO,
        .sda_pullup_en = GPIO_PULLUP_ENABLE,
        .scl_pullup_en = GPIO_PULLUP_ENABLE,
        .master.clk_speed = I2C_FREQ_HZ
    };
    ESP_ERROR_CHECK(i2c_param_config(I2C_PORT, &conf));
    return i2c_driver_install(I2C_PORT, I2C_MODE_MASTER, 0, 0, 0);
}

static esp_err_t ina219_write_u16(uint8_t reg, uint16_t val)
{
    i2c_cmd_handle_t cmd = i2c_cmd_link_create();
    esp_err_t err = ESP_OK;
    uint8_t msb = (uint8_t)(val >> 8);
    uint8_t lsb = (uint8_t)(val & 0xFF);

    i2c_master_start(cmd);
    i2c_master_write_byte(cmd, (INA219_ADDR << 1) | I2C_MASTER_WRITE, true);
    i2c_master_write_byte(cmd, reg, true);
    i2c_master_write_byte(cmd, msb, true);
    i2c_master_write_byte(cmd, lsb, true);
    i2c_master_stop(cmd);

    err = i2c_master_cmd_begin(I2C_PORT, cmd, pdMS_TO_TICKS(20));
    i2c_cmd_link_delete(cmd);
    return err;
}

static esp_err_t ina219_read_u16(uint8_t reg, uint16_t *out)
{
    if (!out) return ESP_ERR_INVALID_ARG;

    i2c_cmd_handle_t cmd = i2c_cmd_link_create();
    esp_err_t err = ESP_OK;
    uint8_t msb=0, lsb=0;

    i2c_master_start(cmd);
    i2c_master_write_byte(cmd, (INA219_ADDR << 1) | I2C_MASTER_WRITE, true);
    i2c_master_write_byte(cmd, reg, true);
    i2c_master_stop(cmd);
    err = i2c_master_cmd_begin(I2C_PORT, cmd, pdMS_TO_TICKS(20));
    i2c_cmd_link_delete(cmd);
    if (err != ESP_OK) return err;

    cmd = i2c_cmd_link_create();
    i2c_master_start(cmd);
    i2c_master_write_byte(cmd, (INA219_ADDR << 1) | I2C_MASTER_READ, true);
    i2c_master_read_byte(cmd, &msb, I2C_MASTER_ACK);
    i2c_master_read_byte(cmd, &lsb, I2C_MASTER_NACK);
    i2c_master_stop(cmd);
    err = i2c_master_cmd_begin(I2C_PORT, cmd, pdMS_TO_TICKS(20));
    i2c_cmd_link_delete(cmd);
    if (err != ESP_OK) return err;

    *out = ((uint16_t)msb << 8) | lsb;
    return ESP_OK;
}

static esp_err_t ina219_init(void)
{
    (void)ina219_write_u16(INA219_REG_CONFIG, 0x8000);
    vTaskDelay(pdMS_TO_TICKS(2));

    // 0x3C47 matches RX side config 
    esp_err_t err = ina219_write_u16(INA219_REG_CONFIG, 0x3C47);
    if (err == ESP_OK) {
        ESP_LOGI("LASER", "INA219 configured (0x3C47), shunt=%.3f ohm", (double)LASER_SHUNT_OHMS);
    }
    return err;
}

static esp_err_t ina219_read_current_amps(float *amps_out)
{
    if (!amps_out) return ESP_ERR_INVALID_ARG;
    uint16_t raw;
    esp_err_t err = ina219_read_u16(INA219_REG_SHUNT, &raw);
    if (err != ESP_OK) return err;

    int16_t sraw = (int16_t)raw;
    float v_shunt = (float)sraw * 0.00001f;
    float current = v_shunt / LASER_SHUNT_OHMS;
    *amps_out = current;
    return ESP_OK;
}

// probe for device 
static bool ina219_probe(void)
{
    uint16_t cfg = 0;
    esp_err_t err = ina219_read_u16(INA219_REG_CONFIG, &cfg);
    if (err == ESP_OK) {
        ESP_LOGI("LASER", "INA219 present, config=0x%04X", cfg);
        return true;
    } else {
        ESP_LOGW("LASER", "INA219 not detected (%s). Running WITHOUT current guard.", esp_err_to_name(err));
        return false;
    }
}

static void laser_guard_task(void *arg)
{
    TickType_t period = pdMS_TO_TICKS(1000 / LASER_POLL_HZ); 
    if (period < 1) {
        period = 1;  // never let it be 0 ticks to avoid crash
    }

    TickType_t last = xTaskGetTickCount();

    ESP_LOGI("LASER",
             "Laser guard start: limit=%.3f A poll=%d Hz (~%u ticks)",
             (double)LASER_I_LIMIT_A,
             (int)LASER_POLL_HZ,
             (unsigned)period);

    for (;;) {
        float Iamp = 0.0f;
        esp_err_t err = ina219_read_current_amps(&Iamp);

        if (err == ESP_OK) {
            if (!g_laser_tripped && Iamp > LASER_I_LIMIT_A) {
                g_laser_tripped = true;

                led_error_on();  // yellow LED latched on
                ESP_LOGE("LASER",
                         "OVERCURRENT! I=%.3f A (limit=%.3f A). TX DISABLED (latched).",
                         (double)Iamp,
                         (double)LASER_I_LIMIT_A);
            }
        } else {
            static uint32_t fail = 0;
            if ((fail++ % 100) == 0) {
                ESP_LOGW("LASER",
                         "INA219 read failed after init: %s",
                         esp_err_to_name(err));
            }
        }

        vTaskDelayUntil(&last, period);
    }
}


/* ------------------ Physical pins --------------- */
#define TX_GPIO           GPIO_NUM_10    // data line out (laser modulation)
#define ACK_RX_GPIO       GPIO_NUM_11    // data line in (for ACKs)
#define TX_IDLE_LEVEL     0              // idle low
#define TX_PRE_IDLE_US    (HALF_BIT_US)  // settle before START
#define TX_POST_IDLE_US   0

/* I2S pins (unconnected) */
#define I2S_BCK_PIN       1
#define I2S_WS_PIN        2

/* ===================== Framing & Types ===================== */
#define FRAME_START   0xAA
#define FRAME_END     0x55
#define FRAME_ESC     0x7D

#define TYPE_KEYB     0x4B /* 'K' */
#define TYPE_MOUSE    0x4D /* 'M' */
#define TYPE_MSC      0x53 /* 'S' */
#define TYPE_ALIGN    0x41 /* 'A' */
#define TYPE_ACK      0x52 /* 'R' */

// align bytes to match align sequence on RX side, must match.
#define ALIGN_0  0xC3
#define ALIGN_1  0x5A
#define ALIGN_2  0xA5
#define ALIGN_3  0x3C

/* ===================== Link / Beacon state ===================== */
static volatile bool g_beacons_enabled = true;     // start beaconing on boot
static volatile bool g_link_ready      = false;    // set to true  after ACK RX

/* ===================== Logging (buffered) ===================== */
#ifndef TXLOG_ENABLE
#define TXLOG_ENABLE 1
#endif
#ifndef TXLOG_BUFFER_SIZE
#define TXLOG_BUFFER_SIZE 4096
#endif
#ifndef TX_BYTE_FORCE_IDLE
#define TX_BYTE_FORCE_IDLE 0
#endif

#if TXLOG_ENABLE
static char  txlog_buf[TXLOG_BUFFER_SIZE];
static size_t txlog_len = 0;
static inline void txlog_begin(const char *prefix) {
    txlog_len = 0;
    if (prefix && *prefix) {
        int wrote = snprintf(txlog_buf, sizeof(txlog_buf), "%s  ", prefix);
        if (wrote < 0) wrote = 0;
        txlog_len = (size_t)wrote;
        if (txlog_len > sizeof(txlog_buf)) txlog_len = sizeof(txlog_buf);
    }
}
static inline void txlog_byte(uint8_t b) {
    if (txlog_len + 9 >= sizeof(txlog_buf)) return;
    for (int i = 7; i >= 0; i--) txlog_buf[txlog_len++] = (b & (1 << i)) ? '1' : '0';
    txlog_buf[txlog_len++] = ' ';
}
static inline void txlog_end(void) {
    if (txlog_len + 1 < sizeof(txlog_buf)) txlog_buf[txlog_len++] = '\n';
    fwrite(txlog_buf, 1, txlog_len, stdout);
    fflush(stdout);
}
#else
static inline void txlog_begin(const char *prefix) {(void)prefix;}
static inline void txlog_byte(uint8_t b) {(void)b;}
static inline void txlog_end(void) {}
#endif

/* ============================ CRC32 ============================= */
static uint32_t crc32_update(uint32_t crc, const uint8_t *data, size_t len) {
    crc = ~crc;
    for (size_t i = 0; i < len; i++) {
        crc ^= data[i];
        for (int k = 0; k < 8; k++) {
            uint32_t mask = -(crc & 1u);
            crc = (crc >> 1) ^ (0xEDB88320u & mask);
        }
    }
    return ~crc;
}

/* ===================== Manchester TX helpers===================== */
static inline void send_manchester_bit(bool bit)
{
    if (!g_tx_allowed) {
        gpio_set_level(TX_GPIO, 0);
        esp_rom_delay_us(HALF_BIT_US);
        esp_rom_delay_us(HALF_BIT_US);
        return;
    }

    if (bit) {
        // '1' = low to high
        gpio_set_level(TX_GPIO, 0);
        esp_rom_delay_us(HALF_BIT_US);
        gpio_set_level(TX_GPIO, 1);
        esp_rom_delay_us(HALF_BIT_US);
    } else {
        // '0' = high to low
        gpio_set_level(TX_GPIO, 1);
        esp_rom_delay_us(HALF_BIT_US);
        gpio_set_level(TX_GPIO, 0);
        esp_rom_delay_us(HALF_BIT_US);
    }
}

static inline void send_byte(uint8_t byte)
{
    for (int i = 7; i >= 0; i--) send_manchester_bit((byte >> i) & 1);
}

/* ---- escaping unified frame ---- */
static inline void tx_put_escaped(uint8_t b) {
    if (b == FRAME_START || b == FRAME_END || b == FRAME_ESC) {
        send_byte(FRAME_ESC);   txlog_byte(FRAME_ESC);
        uint8_t escd = b ^ 0x20;
        send_byte(escd);        txlog_byte(escd);
    } else {
        send_byte(b);           txlog_byte(b);
    }
}

static uint8_t g_seq = 0;
/* ===== Stop-and-Wait ARQ ===== */
static volatile bool     g_awaiting_ack     = false;
static volatile uint8_t  g_waiting_seq      = 0;   // exact seq we expect an ACK for
static volatile bool     g_suppress_seq_inc = false; /* prevents send_frame from bumping g_seq */
static uint8_t           g_pending_type     = 0;
static uint8_t           g_pending_len      = 0;
static uint8_t           g_pending_payload[8]; /* for keyboard (8) and mouse (5) */
static volatile bool     g_has_pending      = false;
static int64_t           g_last_tx_time_us  = 0;

#ifndef RETX_TIMEOUT_US
#define RETX_TIMEOUT_US (INTERFRAME_US*3 + 2000)
#endif

/* Core frame TX also drives RED LED */
static void send_frame(uint8_t type, const uint8_t *payload, uint8_t len,
                       const char *human_prefix)
{
    uint8_t header_seq = g_awaiting_ack ? g_waiting_seq : g_seq;

    uint8_t  hdr[3] = { type, header_seq, len };
    uint32_t crc = 0;
    crc = crc32_update(crc, hdr, sizeof(hdr));
    crc = crc32_update(crc, payload, len);

    txlog_begin(human_prefix);

    // Pre-frame idle settle
    gpio_set_level(TX_GPIO, TX_IDLE_LEVEL);
    if (TX_PRE_IDLE_US > 0) esp_rom_delay_us(TX_PRE_IDLE_US);

    // TX activity RED ON
    led_tx_on();

    send_byte(FRAME_START); txlog_byte(FRAME_START); // START
    for (int i = 0; i < 3; i++) tx_put_escaped(hdr[i]); // header
    for (uint8_t i = 0; i < len; i++) tx_put_escaped(payload[i]); // payload

    // CRC32 (LSB first)
    tx_put_escaped((uint8_t)(crc      ));
    tx_put_escaped((uint8_t)(crc >>  8));
    tx_put_escaped((uint8_t)(crc >> 16));
    tx_put_escaped((uint8_t)(crc >> 24));
    send_byte(FRAME_END); txlog_byte(FRAME_END); // END

    // Post-frame idle and logging
    gpio_set_level(TX_GPIO, TX_IDLE_LEVEL);
    if (TX_POST_IDLE_US > 0) esp_rom_delay_us(TX_POST_IDLE_US);
    txlog_end();

    // TX done RED OFF
    led_tx_off();

    // Inter-frame gap (holding idle) — short busy-wait, then yield
    esp_rom_delay_us(INTERFRAME_US);
    taskYIELD();  // let Idle run

    // Advance seq ONLY for brand-new transmissions
    if (!g_awaiting_ack && !g_suppress_seq_inc) {
        g_seq++;
    }
}

/* ===================== Beacons (alignment) ===================== */
#define ALIGN_FRAME_INTERVAL_MS  2   // spacing between beacons

static void send_align_beacon(void)
{
    static uint32_t ctr = 0;
    uint8_t pl[8] = {
        ALIGN_0, ALIGN_1, ALIGN_2, ALIGN_3,
        (uint8_t)(ctr), (uint8_t)(ctr>>8), (uint8_t)(ctr>>16), (uint8_t)(ctr>>24)
    };
    /* do not consume seq space for ALIGN */
    g_suppress_seq_inc = true;
    send_frame(TYPE_ALIGN, pl, sizeof(pl), "A:beacon");
    g_suppress_seq_inc = false;

    ctr++;
}

static void align_beacon_task(void *arg)
{
    led_set_align(false);

    TickType_t period = pdMS_TO_TICKS(ALIGN_FRAME_INTERVAL_MS);
    if (period < 1) period = 10;                 // avoid 0-tick assert
    TickType_t last = xTaskGetTickCount();

    while (g_beacons_enabled) {
        send_align_beacon();
        vTaskDelayUntil(&last, period);         // steady cadence, yields CPU
    }

    led_set_align(true);                        // indicate link-up on TX side too
    vTaskDelete(NULL);
}

/* ===================== HID wrappers (GATED by g_link_ready) ===================== */
static bool warn_drop_once = false;

static inline bool link_ok_or_warn(void)
{
    if (g_link_ready) return true;
    if (!warn_drop_once) {
        ESP_LOGW("LINK", "Link not established yet — dropping HID/MSC TX until ACK is received.");
        warn_drop_once = true;
    }
    return false;
}

static void send_keyboard_boot_report(const hid_keyboard_input_report_boot_t *kb)
{
    if (!link_ok_or_warn()) return;

    uint8_t payload[8];
    payload[0] = kb->modifier.val;
    payload[1] = 0x00;
    for (int i = 0; i < 6; i++) payload[2 + i] = kb->key[i];

    memcpy(g_pending_payload, payload, 8);
    g_pending_type = TYPE_KEYB;
    g_pending_len  = 8;
    g_has_pending  = true;

    if (!g_awaiting_ack) {
        char prefix[64];
        snprintf(prefix, sizeof(prefix),
                 "K:mod=%02X keys=%02X %02X %02X %02X %02X %02X",
                 payload[0], payload[2], payload[3], payload[4],
                 payload[5], payload[6], payload[7]);

        g_waiting_seq = g_seq;

        g_suppress_seq_inc = true;
        send_frame(TYPE_KEYB, payload, 8, prefix);
        g_suppress_seq_inc = false;

        g_awaiting_ack     = true;
        g_last_tx_time_us  = esp_timer_get_time();
    }
}

static void send_mouse_report(const uint8_t *data, int len)
{
    if (!link_ok_or_warn()) return;
    if (len < 3) return;

    int idx = 0;
    uint8_t report_id = 0;
    if (data[0] >= 1 && data[0] <= 3) { report_id = data[0]; idx = 1; }

    uint8_t buttons = data[idx + 0];
    int8_t  dx      = (int8_t)data[idx + 1];
    int8_t  dy      = (int8_t)data[idx + 2];
    int8_t  v_wh    = (len > idx + 3) ? (int8_t)data[idx + 3] : 0;
    int8_t  h_wh    = (len > idx + 4) ? (int8_t)data[idx + 4] : 0;

    uint8_t payload[5] = { buttons, (uint8_t)dx, (uint8_t)dy, (uint8_t)v_wh, (uint8_t)h_wh };
    uint8_t out_len = 5;

    memcpy(g_pending_payload, payload, out_len);
    g_pending_type = TYPE_MOUSE;
    g_pending_len  = out_len;
    g_has_pending  = true;

    if (!g_awaiting_ack) {
        char prefix[96];
        if (report_id) {
            snprintf(prefix, sizeof(prefix),
                     "M:id=%u btn=%02X dx=%+03d dy=%+03d whV=%+03d whH=%+03d",
                     report_id, buttons, dx, dy, v_wh, h_wh);
        } else {
            snprintf(prefix, sizeof(prefix),
                     "M:btn=%02X dx=%+03d dy=%+03d whV=%+03d whH=%+03d",
                     buttons, dx, dy, v_wh, h_wh);
        }

        /* latch the seq we expect in the ACK */
        g_waiting_seq = g_seq;
        g_suppress_seq_inc = true;
        send_frame(TYPE_MOUSE, payload, out_len, prefix);
        g_suppress_seq_inc = false;
        g_awaiting_ack     = true;
        g_last_tx_time_us  = esp_timer_get_time();
    }
}

/*  USB host main  */
static const char *TAG = "MANCHESTER_TX";

QueueHandle_t app_event_queue = NULL;

typedef enum { APP_EVENT = 0, APP_EVENT_HID_HOST } app_event_group_t;

typedef struct {
    app_event_group_t event_group;
    struct {
        hid_host_device_handle_t handle;
        hid_host_driver_event_t event;
        void *arg;
    } hid_host_device;
} app_event_queue_t;

static const char *hid_proto_name_str[] = { "NONE", "KEYBOARD", "MOUSE" };

static void hid_print_new_device_report_header(hid_protocol_t proto)
{
    static hid_protocol_t prev_proto_output = -1;
    if (prev_proto_output != proto) {
        prev_proto_output = proto;
        printf("\r\n");
        if (proto == HID_PROTOCOL_MOUSE) printf("Mouse\r\n");
        else if (proto == HID_PROTOCOL_KEYBOARD) printf("Keyboard\r\n");
        else printf("Generic\r\n");
        fflush(stdout);
    }
}

static void hid_host_keyboard_report_callback(const uint8_t *const data, const int length)
{
    if (length < sizeof(hid_keyboard_input_report_boot_t)) return;
    const hid_keyboard_input_report_boot_t *kb_report =
        (const hid_keyboard_input_report_boot_t *)data;
    hid_print_new_device_report_header(HID_PROTOCOL_KEYBOARD);
    send_keyboard_boot_report(kb_report);
}

static void hid_host_mouse_report_callback(const uint8_t *const data, const int length)
{
    hid_print_new_device_report_header(HID_PROTOCOL_MOUSE);
    send_mouse_report(data, length);
}

static void hid_host_generic_report_callback(const uint8_t *const data, const int length)
{
    hid_print_new_device_report_header(HID_PROTOCOL_NONE);
    for (int i = 0; i < length; i++) printf("%02X", data[i]);
    putchar('\r');
}

void hid_host_interface_callback(hid_host_device_handle_t hid_device_handle,
                                 const hid_host_interface_event_t event,
                                 void *arg)
{
    uint8_t data[64] = {0};
    size_t data_length = 0;
    hid_host_dev_params_t dev_params;
    ESP_ERROR_CHECK(hid_host_device_get_params(hid_device_handle, &dev_params));

    switch (event) {
    case HID_HOST_INTERFACE_EVENT_INPUT_REPORT:
        ESP_ERROR_CHECK(hid_host_device_get_raw_input_report_data(hid_device_handle, data, 64, &data_length));
        switch (dev_params.proto) {
            case HID_PROTOCOL_KEYBOARD: hid_host_keyboard_report_callback(data, data_length); break;
            case HID_PROTOCOL_MOUSE:    hid_host_mouse_report_callback(data, data_length);    break;
            default:                    hid_host_generic_report_callback(data, data_length);  break;
        }
        break;
    case HID_HOST_INTERFACE_EVENT_DISCONNECTED:
        ESP_LOGI(TAG, "HID Device, protocol '%s' DISCONNECTED", hid_proto_name_str[dev_params.proto]);
        ESP_ERROR_CHECK(hid_host_device_close(hid_device_handle));
        break;
    case HID_HOST_INTERFACE_EVENT_TRANSFER_ERROR:
        ESP_LOGI(TAG, "HID Device, protocol '%s' TRANSFER_ERROR", hid_proto_name_str[dev_params.proto]);
        break;
    default:
        ESP_LOGE(TAG, "HID Device, protocol '%s' Unhandled event", hid_proto_name_str[dev_params.proto]);
        break;
    }
}

void hid_host_device_event(hid_host_device_handle_t hid_device_handle,
                           const hid_host_driver_event_t event,
                           void *arg)
{
    hid_host_dev_params_t dev_params;
    ESP_ERROR_CHECK(hid_host_device_get_params(hid_device_handle, &dev_params));

    switch (event) {
    case HID_HOST_DRIVER_EVENT_CONNECTED: {
        ESP_LOGI(TAG, "HID Device, protocol '%s' CONNECTED", hid_proto_name_str[dev_params.proto]);

        const hid_host_device_config_t dev_config = {
            .callback = hid_host_interface_callback,
            .callback_arg = NULL
        };

        ESP_ERROR_CHECK(hid_host_device_open(hid_device_handle, &dev_config));

        if (HID_SUBCLASS_BOOT_INTERFACE == dev_params.sub_class) {
            if (dev_params.proto == HID_PROTOCOL_MOUSE) {
                esp_err_t err = hid_class_request_set_protocol(hid_device_handle, HID_REPORT_PROTOCOL_REPORT);
                if (err != ESP_OK) {
                    ESP_LOGW(TAG, "Set REPORT protocol failed (%s); falling back to BOOT", esp_err_to_name(err));
                    (void)hid_class_request_set_protocol(hid_device_handle, HID_REPORT_PROTOCOL_BOOT);
                }
            } else if (dev_params.proto == HID_PROTOCOL_KEYBOARD) {
                (void)hid_class_request_set_protocol(hid_device_handle, HID_REPORT_PROTOCOL_BOOT);
                (void)hid_class_request_set_idle(hid_device_handle, 0, 0);
            }
        }

        ESP_ERROR_CHECK(hid_host_device_start(hid_device_handle));
        break;
    }
    default:
        break;
    }
}

/*  ACK RX — Manchester receiver via I2S oversampling  */
#include "driver/i2s_std.h"

/* Logic sample rate (samples of the line per second) */
#define LOGIC_SAMPLE_HZ    (DATA_RATE_BPS * OS_BITS_PER_DATA)
/* I2S: 16-bit mono; BCK = Fs * 16 * 1 */
#define I2S_BITS_PER_WORD  16
#define I2S_NUM_CHANNELS   1
#define I2S_AUDIO_RATE     (LOGIC_SAMPLE_HZ / (I2S_BITS_PER_WORD * I2S_NUM_CHANNELS))

/* DMA sizing (bytes per read = dma_frame_num * 2 since 16-bit samples) */
#define I2S_DMA_FRAME_NUM  1024
#define I2S_DMA_DESC_NUM   8

typedef struct { uint8_t level; uint32_t dur_us; } ack_seg_t;

static i2s_chan_handle_t ack_rx_chan = NULL;

static inline uint32_t u32_le(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1]<<8) | ((uint32_t)p[2]<<16) | ((uint32_t)p[3]<<24);
}

static void i2s_ack_rx_init(void)
{
    // Create RX channel (master), generate BCK/WS, sample DIN=GPIO17 (ACK_RX_GPIO)
    i2s_chan_config_t chan_cfg = I2S_CHANNEL_DEFAULT_CONFIG(I2S_NUM_0, I2S_ROLE_MASTER);
    chan_cfg.dma_desc_num = I2S_DMA_DESC_NUM;
    chan_cfg.dma_frame_num = I2S_DMA_FRAME_NUM;
    ESP_ERROR_CHECK(i2s_new_channel(&chan_cfg, NULL, &ack_rx_chan));

    i2s_std_config_t std_cfg = {
        .clk_cfg = I2S_STD_CLK_DEFAULT_CONFIG(I2S_AUDIO_RATE),
        .slot_cfg = I2S_STD_PHILIPS_SLOT_DEFAULT_CONFIG(I2S_DATA_BIT_WIDTH_16BIT, I2S_SLOT_MODE_MONO),
        .gpio_cfg = {
            .mclk = I2S_GPIO_UNUSED,
            .bclk = I2S_BCK_PIN,
            .ws   = I2S_WS_PIN,
            .dout = I2S_GPIO_UNUSED,
            .din  = ACK_RX_GPIO
        }
    };
    std_cfg.clk_cfg.mclk_multiple = I2S_MCLK_MULTIPLE_256;
    std_cfg.slot_cfg.slot_mask = I2S_STD_SLOT_LEFT;

    // Keep the DIN line idling low and stable
    gpio_config_t pulldown = {
        .pin_bit_mask = 1ULL << ACK_RX_GPIO,
        .mode = GPIO_MODE_INPUT,
        .pull_up_en = GPIO_PULLUP_DISABLE,
        .pull_down_en = GPIO_PULLDOWN_ENABLE,
        .intr_type = GPIO_INTR_DISABLE
    };
    ESP_ERROR_CHECK(gpio_config(&pulldown));

    ESP_ERROR_CHECK(i2s_channel_init_std_mode(ack_rx_chan, &std_cfg));
    ESP_ERROR_CHECK(i2s_channel_enable(ack_rx_chan));

    ESP_LOGI("ACK", "I2S ACK RX: Fs=%u Hz  logic=%u sps  DIN=GPIO%d  BCK=%d WS=%d",
             (unsigned)I2S_AUDIO_RATE, (unsigned)LOGIC_SAMPLE_HZ,
             ACK_RX_GPIO, I2S_BCK_PIN, I2S_WS_PIN);
}

/* Convert I2S capture to segments (level, duration_us) */
static size_t ack_collect_burst_i2s(ack_seg_t *out, size_t cap)
{
    const uint32_t gap_samples =
        (uint32_t)((uint64_t)INTERFRAME_US * LOGIC_SAMPLE_HZ / 1000000ULL);

    size_t n = 0;

    static int last_lvl = -1;
    uint32_t run = 0;

    static int16_t dma_buf[I2S_DMA_FRAME_NUM];
    size_t bytes_read = 0;

    // keep the wait small so we unblock often
    const TickType_t max_wait = pdMS_TO_TICKS(5);
    int idle_loops   = 0;
    int slice_budget = 0;

    while (n < cap) {
        esp_err_t err = i2s_channel_read(ack_rx_chan, dma_buf, sizeof(dma_buf),
                                         &bytes_read, max_wait);
        if (err != ESP_OK || bytes_read == 0) {
            if (++idle_loops > 5) break;
            // sleep so IDLE1 runs
            vTaskDelay(1);
            continue;
        }
        idle_loops = 0;

        // keep each call bounded in CPU time
        if (++slice_budget >= 4) break;   // at most 4 DMA buffers per call

        size_t nsamp16 = bytes_read / sizeof(int16_t);
        for (size_t i = 0; i < nsamp16; i++) {
            uint16_t w = (uint16_t)dma_buf[i];
            for (int b = 15; b >= 0; --b) {
                int lvl = (w >> b) & 1;

                if (last_lvl < 0) { last_lvl = lvl; run = 1; continue; }

                if (lvl == last_lvl) {
                    run++;
                } else {
                    if (run > 0 && n < cap) {
                        uint32_t dur_us = (uint32_t)((uint64_t)run * 1000000ULL / LOGIC_SAMPLE_HZ);
                        if (dur_us == 0) dur_us = 1;
                        out[n++] = (ack_seg_t){ .level = (uint8_t)last_lvl, .dur_us = dur_us };
                    }
                    last_lvl = lvl;
                    run = 1;
                }

                if (run >= gap_samples && n >= 12) goto flush_and_exit;
            }
        }

        // yield between DMA buffers, even when busy
        taskYIELD();
    }

flush_and_exit:
    if (run > 0 && n < cap) {
        uint32_t dur_us = (uint32_t)((uint64_t)run * 1000000ULL / LOGIC_SAMPLE_HZ);
        if (dur_us == 0) dur_us = 1;
        out[n++] = (ack_seg_t){ .level = (uint8_t)last_lvl, .dur_us = dur_us };
    }
    return n;
}

/* segments to half-bit level samples (reject tiny glitches) */
static size_t ack_segs_to_halves(const ack_seg_t *in, size_t n_in, uint8_t *halves, size_t cap, uint32_t half_us)
{
    size_t n=0;
    for (size_t i=0;i<n_in;i++) {
        float r = (float)in[i].dur_us / (float)half_us;
        if (r < 0.75f) continue;            // reject glitches or duration less than 75% of expected
        int cnt = (int)(r + 0.5f);
        if (cnt < 1) cnt = 1;
        if (cnt > 8) cnt = 8;
        uint8_t lvl = in[i].level ? 1 : 0;
        for (int k=0;k<cnt && n<cap;k++) halves[n++] = lvl;
        if (n >= cap) break;
    }
    return n;
}

/* halves to bits. Try with and without inversion. */
static size_t ack_halves_to_bits_inv(const uint8_t *h, size_t n_h,
                                     uint8_t *bits, size_t cap, int invert)
{
    size_t i = 0, nb = 0;
    while (i + 1 < n_h && nb < cap) {
        uint8_t a = h[i]   ^ (invert ? 1 : 0);
        uint8_t b = h[i+1] ^ (invert ? 1 : 0);
        if      (a == 0 && b == 1) { bits[nb++] = 1; i += 2; } // 1 = low to high
        else if (a == 1 && b == 0) { bits[nb++] = 0; i += 2; } // 0 = high to low
        else { i += 1; }
    }
    return nb;
}

/* Pack bits into bytes starting at bit offset `off` (0..7), MSB-first */
static size_t ack_bits_to_bytes_msb_off(const uint8_t *bits, size_t n_bits, int off, uint8_t *bytes, size_t cap)
{
    if (off < 0) off = 0;
    if (off > 7) off = 7;
    if (n_bits <= (size_t)off) return 0;
    size_t usable = n_bits - (size_t)off;
    size_t n = usable / 8;
    if (n > cap) n = cap;
    size_t bi = (size_t)off;
    for (size_t i=0;i<n;i++) {
        uint8_t b=0;
        for (int k=0;k<8;k++,bi++) b = (b<<1) | (bits[bi] & 1);
        bytes[i]=b;
    }
    return n;
}

static bool ack_find_frame(const uint8_t *bytes, size_t n, size_t *s, size_t *e)
{
    for (size_t i=0;i+2<n;i++) {
        if (bytes[i]==FRAME_START) {
            for (size_t j=i+2;j<n;j++) if (bytes[j]==FRAME_END) { *s=i; *e=j; return true; }
        }
    }
    return false;
}

static size_t ack_deescape(const uint8_t *bytes, size_t s, size_t e, uint8_t *out, size_t cap)
{
    size_t n=0;
    for (size_t i=s+1;i<e;i++) {
        uint8_t b=bytes[i];
        if (b==FRAME_ESC) {
            if (i+1 >= e) return 0;
            uint8_t x = bytes[++i] ^ 0x20;
            if (n<cap) out[n++]=x; else return 0;
        } else {
            if (n<cap) out[n++]=b; else return 0;
        }
    }
    return n;
}

/* Latch green LED */
static void on_ack_received(uint8_t seq)
{
    /* finalise only when ACK matches the exact latched seq */
    if (g_awaiting_ack && seq == g_waiting_seq) {
        g_awaiting_ack    = false;
        g_last_tx_time_us = 0;
        g_seq++; /* advance only on correct ACK */

        if (g_has_pending) {
            char prefix[96] = {0};
            if (g_pending_type == TYPE_KEYB && g_pending_len == 8) {
                snprintf(prefix, sizeof(prefix),
                         "K:mod=%02X keys=%02X %02X %02X %02X %02X %02X",
                         g_pending_payload[0], g_pending_payload[2], g_pending_payload[3],
                         g_pending_payload[4], g_pending_payload[5], g_pending_payload[6],
                         g_pending_payload[7]);
            } else if (g_pending_type == TYPE_MOUSE) {
                snprintf(prefix, sizeof(prefix), "M:coalesced");
            } else {
                snprintf(prefix, sizeof(prefix), "D:coalesced");
            }

            /* latch seq for the next send */
            g_waiting_seq      = g_seq;
            g_suppress_seq_inc = true;
            send_frame(g_pending_type, g_pending_payload, g_pending_len, prefix);
            g_suppress_seq_inc = false;
            g_awaiting_ack     = true;
            g_last_tx_time_us  = esp_timer_get_time();
            g_has_pending      = false;
            return;
        }
    }

    /* allow simple ACKs to enable link */
    enum { ACKS_REQUIRED = 1, WINDOW_US = 50000 };
    static int     cnt = 0;
    static int64_t t0  = 0;

    int64_t now = esp_timer_get_time();
    if (t0 == 0 || (now - t0) > WINDOW_US) { t0 = now; cnt = 0; }
    cnt++;

    if (!g_link_ready && cnt >= ACKS_REQUIRED) {
        g_link_ready      = true;
        g_beacons_enabled = false;  // beacon task exits
        led_set_align(true);        // latch green

        /* reset reliable sequence numbering now that link is up */
        g_seq           = 0;
        g_awaiting_ack  = false;
        g_has_pending   = false;

        ESP_LOGI("LINK", "ACKs=%d/%d in %dms — LINK UP (last seq=%u).",
                 cnt, ACKS_REQUIRED, (int)((now - t0)/1000), seq);
    }
}

static void ack_rx_task(void *arg)
{
    i2s_ack_rx_init();

    // Working buffers
    const uint32_t HALF_US = HALF_BIT_US;

    ack_seg_t segs[2048];
    uint8_t halves[4096];
    uint8_t bits[4096];
    uint8_t bytes[2048];
    uint8_t frame[64];

    ESP_LOGI(TAG, "ACK RX listening (I2S) on GPIO%d", ACK_RX_GPIO);

    while (1) {
        size_t n_segs = ack_collect_burst_i2s(segs, 2048);
        if (n_segs < 12) { vTaskDelay(pdMS_TO_TICKS(2)); continue; }

        size_t n_halves = ack_segs_to_halves(segs, n_segs, halves, 4096, HALF_US);

        bool found = false;
        size_t s=0,e=0;
        int used_off = -1, used_inv = -1;
        size_t n_bits = 0;

        for (int inv=0; inv<=1 && !found; inv++) {
            n_bits = ack_halves_to_bits_inv(halves, n_halves, bits, 4096, inv);
            for (int off=0; off<8 && !found; off++) {
                size_t n_bytes = ack_bits_to_bytes_msb_off(bits, n_bits, off, bytes, 2048);
                if (n_bytes < 8) continue;
                if (ack_find_frame(bytes, n_bytes, &s, &e)) {
                    used_inv = inv;
                    used_off = off;
                    found = true;
                }
            }
        }
        if (!found) { vTaskDelay(pdMS_TO_TICKS(1)); continue; }

        size_t fr_len = ack_deescape(bytes, s, e, frame, sizeof(frame));
        if (fr_len < 3+4) { vTaskDelay(pdMS_TO_TICKS(1)); continue; }

        uint8_t type = frame[0];
        uint8_t seq  = frame[1];
        uint8_t plen = frame[2];
        size_t needed = 3 + (size_t)plen + 4;
        if (fr_len < needed) { vTaskDelay(pdMS_TO_TICKS(1)); continue; }

        uint32_t crc_rx = u32_le(&frame[3 + plen]);
        uint32_t crc_ok = crc32_update(0, frame, 3 + plen);
        if (crc_ok != crc_rx) { vTaskDelay(pdMS_TO_TICKS(1)); continue; }

        if (type == TYPE_ACK) {
            static bool printed_once = false;
            if (!printed_once) {
                ESP_LOGI(TAG, "ACK RX using invert=%d, bit_offset=%d", used_inv, used_off);
                printed_once = true;
            }
            ESP_LOGI(TAG, "ACK RX: seq=%u len=%u CRC=OK", seq, plen);
            on_ack_received(seq);
        }
    }
}

/* Retransmission timer task */
static void tx_retx_task(void *arg)
{
    // Base timeout: over a full frame @60 kb/s, absorbs RTOS/USB jitter
    const int64_t   kBaseTimeoutUs = 4000;     // 4 ms
    const int64_t   kMaxTimeoutUs  = 40000;    // 40 ms
    const int       kMaxRetx       = 50;       // used for logging
    const TickType_t kIdlePoll     = pdMS_TO_TICKS(10);

    int     retx_count    = 0;
    int64_t cur_timeout   = kBaseTimeoutUs;
    bool    was_waiting   = false;
    bool    logged_hold   = false;  

    while (1) {
        if (!g_awaiting_ack) {
            if (was_waiting) {
                ESP_LOGI("RETX", "cleared (ACK received).");
                was_waiting = false;
            }
            retx_count   = 0;
            cur_timeout  = kBaseTimeoutUs;
            logged_hold  = false;
            vTaskDelay(kIdlePoll);
            continue;
        }

        // waiting for an ACK
        if (!was_waiting) {
            ESP_LOGW("RETX", "waiting for ACK(seq=%u)…", (unsigned)g_waiting_seq);
            was_waiting = true;
        }

        int64_t now        = esp_timer_get_time();
        int64_t elapsed_us = now - g_last_tx_time_us;
        int64_t remain_us  = cur_timeout - elapsed_us;

        if (remain_us > 0) {
            //sleep until timeout. Always yield at least 1 tick.
            TickType_t wait_ticks = pdMS_TO_TICKS((remain_us / 1000) + 1);
            if (wait_ticks < 1) wait_ticks = 1;
            vTaskDelay(wait_ticks);
            continue;
        }

        // Timeout expired
        retx_count++;
        if (retx_count == 1 || (retx_count % 10) == 0) {
            ESP_LOGW("RETX", "timeout #%d for seq=%u (retrying, t=%d ms)",
                     retx_count, (unsigned)g_waiting_seq, (int)(cur_timeout/1000));
        }

        // If we’ve exceeded the nominal retry count
        if (retx_count > kMaxRetx) {
            if (!logged_hold) {
                ESP_LOGE("RETX", "no ACK after %d retries (seq=%u). Holding frame; continuing retries without resync.",
                         retx_count, (unsigned)g_waiting_seq);
                logged_hold = true;
            }
            retx_count  = kMaxRetx;    
            cur_timeout = kMaxTimeoutUs;
        }

        // Retransmit the same frame with the latched seq.
        const char *pref = NULL;
        g_suppress_seq_inc = true;
        send_frame(g_pending_type, g_pending_payload, g_pending_len, pref);
        g_suppress_seq_inc = false;

        g_last_tx_time_us = esp_timer_get_time();

        // Exponential backoff (bounded)
        cur_timeout += cur_timeout >> 1;  // ×1.5
        if (cur_timeout > kMaxTimeoutUs) cur_timeout = kMaxTimeoutUs;

        // Always yield a tick for the WD
        vTaskDelay(1);
    }
}

/* ===================== USB host  ===================== */
static void usb_lib_task(void *arg)
{
    const usb_host_config_t host_config = {
        .skip_phy_setup = false,
        .intr_flags = ESP_INTR_FLAG_LEVEL1,
    };

    ESP_ERROR_CHECK(usb_host_install(&host_config));
    xTaskNotifyGive(arg);

    while (true) {
        uint32_t event_flags;
        usb_host_lib_handle_events(portMAX_DELAY, &event_flags);
        if (event_flags & USB_HOST_LIB_EVENT_FLAGS_NO_CLIENTS) {
            ESP_ERROR_CHECK(usb_host_device_free_all());
            break;
        }
    }

    ESP_LOGI(TAG, "USB shutdown");
    vTaskDelay(10);
    ESP_ERROR_CHECK(usb_host_uninstall());
    vTaskDelete(NULL);
}

void hid_host_device_callback(hid_host_device_handle_t hid_device_handle,
                              const hid_host_driver_event_t event,
                              void *arg)
{
    const app_event_queue_t evt_queue = {
        .event_group = APP_EVENT_HID_HOST,
        .hid_host_device = { .handle = hid_device_handle, .event = event, .arg = arg }
    };
    if (app_event_queue) xQueueSend(app_event_queue, &evt_queue, 0);
}

/* ===================== MAIN ===================== */
void app_main(void)
{
    // Give power time to settle
    vTaskDelay(pdMS_TO_TICKS(400));

    // LEDs
    led_init();

    // TX pin
    gpio_config_t txio = {
        .pin_bit_mask = 1ULL << TX_GPIO,
        .mode = GPIO_MODE_OUTPUT,
        .pull_up_en = 0,
        .pull_down_en = 0,
        .intr_type = GPIO_INTR_DISABLE
    };
    ESP_ERROR_CHECK(gpio_config(&txio));
    gpio_set_level(TX_GPIO, TX_IDLE_LEVEL);

    ESP_LOGI(TAG,
             "HID Host (Manchester TX)  DATA_RATE=%u bps  BIT_US=%u  HALF=%u us  GAP=%u us",
             (unsigned)DATA_RATE_BPS,
             (unsigned)BIT_DURATION_US,
             (unsigned)HALF_BIT_US,
             (unsigned)INTERFRAME_US);

    // Init I2C bus for INA219
    if (i2c_master_init_simple() != ESP_OK) {
        ESP_LOGE(TAG, "I2C init failed - cannot talk to sensor bus");
        led_error_on();        //yellow LED on
    }

    // Try  init INA219
    esp_err_t ina_err = ina219_init();
    if (ina_err == ESP_OK) {
        g_current_monitor_present = ina219_probe();
    } else {
        ESP_LOGW("LASER", "INA219 init failed (%s). Assuming no current monitor.",
                 esp_err_to_name(ina_err));
        g_current_monitor_present = false;
    }

    if (g_current_monitor_present) {
        // Start guard task for overcurrent protection
        xTaskCreatePinnedToCore(laser_guard_task,
                                "laser_guard",
                                2048,
                                NULL,
                                5,
                                NULL,
                                0);
    } else {
        led_error_off();
        g_tx_allowed = true;
    }

    // USB host task
    BaseType_t t1 = xTaskCreatePinnedToCore(usb_lib_task, "usb_events",
                                            4096, xTaskGetCurrentTaskHandle(), 2, NULL, 0);
    assert(t1 == pdPASS);
    ulTaskNotifyTake(false, pdMS_TO_TICKS(1000));

    const hid_host_driver_config_t hid_host_driver_config = {
        .create_background_task = true,
        .task_priority = 5,
        .stack_size = 4096,
        .core_id = 0,
        .callback = hid_host_device_callback,
        .callback_arg = NULL
    };
    ESP_ERROR_CHECK(hid_host_install(&hid_host_driver_config));

    // Beacon task (Core 0) — watchdog-friendly 
    xTaskCreatePinnedToCore(align_beacon_task, "align_beacon", 2048, NULL, 3, NULL, 0);

    // ACK RX task (Core 1) — I2S oversampling for ACKs
    xTaskCreatePinnedToCore(ack_rx_task, "ack_rx", 4096, NULL, 4, NULL, 1);
    xTaskCreatePinnedToCore(tx_retx_task, "tx_retx", 2048, NULL, 3, NULL, 0);

    // Event queue + loop (HID events etc.)
    app_event_queue = xQueueCreate(10, sizeof(app_event_queue_t));
    ESP_LOGI(TAG, "Waiting for HID Device to be connected");
    app_event_queue_t evt_queue;

    while (1) {
        if (xQueueReceive(app_event_queue, &evt_queue, portMAX_DELAY)) {
            if (APP_EVENT_HID_HOST == evt_queue.event_group) {


                hid_host_device_event(evt_queue.hid_host_device.handle,
                                      evt_queue.hid_host_device.event,
                                      evt_queue.hid_host_device.arg);
            }
        }
    }
}
