#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdarg.h>
#include <math.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"

#include "driver/gpio.h"
#include "driver/i2c.h"
#include "esp_timer.h"
#include "esp_log.h"
#include "esp_heap_caps.h"
#include "esp_rom_sys.h"

// -------- TinyUSB (HID) --------
#include "tinyusb.h"
#include "tusb.h"
#include "class/hid/hid.h"

#define TAG "MANCHESTER_RX"

// ========= Debug switches =========
#define DEBUG_RX            1
#define DEBUG_RATE_LIMIT    8
#define DEBUG_DUMP_HALVES   96
#define DEBUG_DUMP_BITS     96
#define DEBUG_DUMP_BYTES    32

// ===================== rate config =====================
#define DATA_RATE_BPS       60000
#define OS_BITS_PER_DATA    32

// ===================== Derived timings ======================
#define BIT_US         (1000000 / DATA_RATE_BPS)
#define HALF_GUESS_US  (BIT_US / 2)
#define FRAME_GAP_BITS 4
#define FRAME_GAP_US   (BIT_US * FRAME_GAP_BITS)

// Edge/queue sizing
#define EDGES_PER_SEC     (2 * DATA_RATE_BPS)
#define QUEUE_TARGET      (EDGES_PER_SEC / 10)
#define ISR_QUEUE_DEPTH   ((QUEUE_TARGET < 512) ? 512 : (QUEUE_TARGET > 4096 ? 4096 : QUEUE_TARGET))

// ===================== Physical =====================
#define RX_GPIO 17  // input pin connected to TX line (from transmitter)
#define TX_GPIO 18  // output pin back to sender 

#define TYPE_ACK   0x52 // 'R'
#define TYPE_ALIGN 0x41 // 'A'
#define TYPE_KBD   0x4B // 'K'
#define TYPE_MOUSE 0x4D // 'M'

// ===== Stop-and-Wait tracking =====
static bool     seq_locked = false;
static uint8_t  expected_seq = 0;
static uint8_t  last_delivered_seq = 0xFF;
static uint8_t  last_kbd[8] = {0};

// Keyboard send/pending + helpers
static int64_t  last_kbd_tx_us = 0;
static uint8_t  pending_kbd[8] = {0};
static bool     has_pending_kbd = false;

// ===================== LEDs =====================
#define LED_RED_GPIO 7 // RED   = power + activity pulse on valid frame
#define LED_GRN_GPIO 8 // GREEN = alignment lock
#define LED_YEL_GPIO 9 // YELLOW = overcurrent / fatal

#define LED_ACTIVITY_US 30000
static inline void led_pulse_activity_us(uint32_t us);
static inline void led_pulse_kbd(void) { led_pulse_activity_us(15000); }

// ---------------- helpers ----------------
static inline bool is_all_zero8(const uint8_t *p) {
    uint8_t z = 0; for (int i = 0; i < 8; i++) z |= p[i]; return z == 0;
}
static inline size_t min_size(size_t a, size_t b) { return a < b ? a : b; }
static uint32_t u32_le(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static void dump_hex_bytes(const uint8_t *p, size_t n) { for (size_t i = 0; i < n; i++) printf("%02X%s", p[i], (i+1<n) ? " " : ""); printf("\n"); }
static void dump_bin_bytes_line(const uint8_t *p, size_t n) {
    for (size_t i = 0; i < n; i++) { uint8_t b = p[i]; for (int k = 7; k >= 0; k--) putchar((b & (1 << k)) ? '1' : '0'); putchar((i + 1 < n) ? ' ' : '\n'); }
}
static void dump_bits_line(const uint8_t *b, size_t n_bits)
{
    for (size_t i = 0; i < n_bits; i++) { putchar(b[i] ? '1' : '0'); if ((i % 8) == 7) putchar(' '); }
    putchar('\n');
}
static void dump_halves_line(const uint8_t *h, size_t n_halves)
{
    for (size_t i = 0; i < n_halves; i++) { putchar(h[i] ? '1' : '0'); if ((i % 16) == 15) putchar(' '); }
    putchar('\n');
}

// ===================== INA219 LASER CURRENT GUARD =====================
// I2C setup
#define I2C_PORT        I2C_NUM_0
#define I2C_SDA_GPIO    4
#define I2C_SCL_GPIO    5
#define I2C_FREQ_HZ     400000

// INA219
#define INA219_ADDR     0x40
#define INA219_REG_CONFIG  0x00
#define INA219_REG_SHUNT   0x01
#define INA219_REG_BUS     0x02

// Laser control
#define LASER_EN_GPIO       12      
#define LASER_I_LIMIT_A     0.03f   // trip at 30 mA
#define LASER_SHUNT_OHMS    0.1f
#define LASER_POLL_HZ       2000     // 2000 Hz poll

// Latch flags
static volatile bool g_laser_tripped = false;
static volatile bool g_tx_allowed    = true;  // *** CHANGED *** gate optical output like TX.c
static bool g_current_monitor_present = false;

static inline void laser_enable(bool en)
{
    gpio_set_level(LASER_EN_GPIO, en ? 1 : 0);
}

static void laser_gpio_init(void)
{
    gpio_config_t io = {
        .pin_bit_mask = (1ULL << LASER_EN_GPIO),
        .mode = GPIO_MODE_OUTPUT,
        .pull_up_en = GPIO_PULLUP_DISABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type = GPIO_INTR_DISABLE
    };
    gpio_config(&io);

    // ON at boot
    laser_enable(true);
}

// I2C helpers
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

    // read two bytes
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
    esp_err_t err = ina219_write_u16(INA219_REG_CONFIG, 0x3C47);
    if (err == ESP_OK) {
        ESP_LOGI(TAG, "INA219 configured (0x3C47), shunt=%.3f ohm", (double)LASER_SHUNT_OHMS);
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

// detects if device is on I2C.
static bool ina219_probe(void)
{
    uint16_t dummy = 0;
    esp_err_t err = ina219_read_u16(INA219_REG_CONFIG, &dummy);
    if (err == ESP_OK) {
        ESP_LOGI(TAG, "INA219 present, config=0x%04X", dummy);
        return true;
    } else {
        ESP_LOGW(TAG, "INA219 not detected (%s). Running WITHOUT current guard.", esp_err_to_name(err));
        return false;
    }
}

static void laser_guard_task(void *arg)
{

    TickType_t period = pdMS_TO_TICKS(1000 / LASER_POLL_HZ); 
    if (period < 1) {
        period = 1;
    }

    TickType_t last = xTaskGetTickCount();

    ESP_LOGI(TAG,
             "Laser guard start: limit=%.3f A, poll=%d Hz (~%u ticks)",
             (double)LASER_I_LIMIT_A,
             (int)LASER_POLL_HZ,
             (unsigned)period);

    for (;;)
    {
        float Iamp = 0.0f;
        esp_err_t err = ina219_read_current_amps(&Iamp);

        if (err == ESP_OK) {

            if (!g_laser_tripped && Iamp > LASER_I_LIMIT_A) {
                // HARD TRIP
                g_laser_tripped = true;
                laser_enable(false);
                g_tx_allowed = false;

                // drive TX low so no light is emitted
                gpio_set_level(TX_GPIO, 0);

                // latch yellow LED
                gpio_set_level(LED_YEL_GPIO, 1);

                ESP_LOGE(TAG,
                         "LASER OVERCURRENT! I=%.3f A (limit=%.3f A). OUTPUT DISABLED (latched).",
                         (double)Iamp,
                         (double)LASER_I_LIMIT_A);
            }

        } else {

            static uint32_t fail = 0;
            if ((fail++ % 100) == 0) {
                ESP_LOGW(TAG, "INA219 read failed after init: %s", esp_err_to_name(err));
            }
        }

        vTaskDelayUntil(&last, period);
    }
}

// ===================== CRC32 =====================
static uint32_t crc32_update(uint32_t crc, const uint8_t *data, size_t len)
{
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

// ===================== LEDs implementation =====================
static volatile int64_t g_red_until_us = 0;
static TaskHandle_t g_fatal_blink_task = NULL;

static inline void leds_init(void)
{
    gpio_config_t io = {
        .pin_bit_mask = (1ULL << LED_RED_GPIO) | (1ULL << LED_GRN_GPIO) | (1ULL << LED_YEL_GPIO),
        .mode = GPIO_MODE_OUTPUT,
        .pull_up_en = GPIO_PULLUP_DISABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type = GPIO_INTR_DISABLE
    };
    gpio_config(&io);
    // Power-on: RED solid, others off at start
    gpio_set_level(LED_RED_GPIO, 1);
    gpio_set_level(LED_GRN_GPIO, 0);
    gpio_set_level(LED_YEL_GPIO, 0);
}
static inline void led_pulse_activity_us(uint32_t us)
{
    int64_t now = esp_timer_get_time();
    g_red_until_us = now + us;
    gpio_set_level(LED_RED_GPIO, 1);
}
static inline void fatal_led_on_now(void) { gpio_set_level(LED_YEL_GPIO, 1); }
static void fatal_blink_task(void *arg)
{
    int code = (int)(intptr_t)arg; if (code <= 0) code = 1;
    while (true) {
        for (int i = 0; i < code; i++) {
            gpio_set_level(LED_YEL_GPIO, 1); vTaskDelay(pdMS_TO_TICKS(140));
            gpio_set_level(LED_YEL_GPIO, 0); vTaskDelay(pdMS_TO_TICKS(140));
        }
        vTaskDelay(pdMS_TO_TICKS(700));
    }
}
static inline void fatal_blink_start(int code)
{
    if (g_fatal_blink_task == NULL)
        xTaskCreatePinnedToCore(fatal_blink_task, "fatal_blink", 2048,
                                (void *)(intptr_t)code, 3, &g_fatal_blink_task, 0);
}
// maintain RED pulse
static inline void leds_maintain(void)
{
    int64_t now = esp_timer_get_time();
    if (g_red_until_us && now >= g_red_until_us) {
        gpio_set_level(LED_RED_GPIO, 0);
        g_red_until_us = 0;
    }
}

// ---------------- Manchester TX (ACK) ----------------
// *** CHANGED *** gate modulation on RX side too
static inline void ack_send_manchester_bit(bool bit)
{
    if (!g_tx_allowed) {
        gpio_set_level(TX_GPIO, 0);
        esp_rom_delay_us(HALF_GUESS_US);
        esp_rom_delay_us(HALF_GUESS_US);
        return;
    }

    if (bit) {
        // '1' = low -> high
        gpio_set_level(TX_GPIO, 0);
        esp_rom_delay_us(HALF_GUESS_US);
        gpio_set_level(TX_GPIO, 1);
        esp_rom_delay_us(HALF_GUESS_US);
    } else {
        // '0' = high -> low
        gpio_set_level(TX_GPIO, 1);
        esp_rom_delay_us(HALF_GUESS_US);
        gpio_set_level(TX_GPIO, 0);
        esp_rom_delay_us(HALF_GUESS_US);
    }
}

static inline void ack_send_byte(uint8_t b)
{
    for (int i = 7; i >= 0; i--) ack_send_manchester_bit((b >> i) & 1);
}
static inline void ack_put_escaped(uint8_t b)
{
    if (b == 0xAA || b == 0x55 || b == 0x7D) { ack_send_byte(0x7D); ack_send_byte(b ^ 0x20); }
    else { ack_send_byte(b); }
}
static void send_ack_frame(uint8_t seq, const char *prefix)
{
    (void)prefix;
    const uint8_t hdr[3] = { TYPE_ACK, seq, 0 };  // len=0
    uint32_t crc = crc32_update(0, hdr, sizeof(hdr));

    ESP_LOGI(TAG, "ACK: send (seq=%u)", seq);

    // settle low
    gpio_set_level(TX_GPIO, 0);
    esp_rom_delay_us(HALF_GUESS_US);

    // START + header + CRC + END
    ack_send_byte(0xAA);
    ack_put_escaped(hdr[0]);
    ack_put_escaped(hdr[1]);
    ack_put_escaped(hdr[2]);
    ack_put_escaped((uint8_t)(crc));
    ack_put_escaped((uint8_t)(crc >> 8));
    ack_put_escaped((uint8_t)(crc >> 16));
    ack_put_escaped((uint8_t)(crc >> 24));
    ack_send_byte(0x55);

    gpio_set_level(TX_GPIO, 0);
    esp_rom_delay_us(FRAME_GAP_US);
}

// ---------------- Manchester RX plumbing  ----------------
typedef struct { uint8_t level; uint32_t dur_us; } seg_t;
typedef seg_t isr_msg_t;

static volatile int g_last_level = 0;
static volatile int64_t g_last_t_us = 0;

//buffer allocation
#define MAX_SEGS   8192
#define MAX_HALVES 8192
#define MAX_BITS   4096
#define MAX_BYTES  (MAX_BITS / 8)

static seg_t   *segs      = NULL;
static uint8_t *halves    = NULL;
static uint8_t *bits      = NULL;
static uint8_t *bytes     = NULL;
static uint8_t *frame_buf = NULL;

// =============== I2S oversampling path ===============
#include "driver/i2s_std.h"

#define LOGIC_SAMPLE_HZ (DATA_RATE_BPS * OS_BITS_PER_DATA)
#define I2S_BITS_PER_WORD 16
#define I2S_NUM_CHANNELS  1
#define I2S_AUDIO_RATE    (LOGIC_SAMPLE_HZ / (I2S_BITS_PER_WORD * I2S_NUM_CHANNELS))

#define I2S_BCK_PIN 1
#define I2S_WS_PIN  2
#define I2S_DIN_PIN RX_GPIO

#define I2S_DMA_FRAME_NUM 256
#define I2S_DMA_DESC_NUM  4

static i2s_chan_handle_t rx_chan = NULL;

static void i2s_rx_init(void)
{
    i2s_chan_config_t chan_cfg = I2S_CHANNEL_DEFAULT_CONFIG(I2S_NUM_0, I2S_ROLE_MASTER);
    chan_cfg.dma_desc_num  = I2S_DMA_DESC_NUM;
    chan_cfg.dma_frame_num = I2S_DMA_FRAME_NUM;
    ESP_ERROR_CHECK(i2s_new_channel(&chan_cfg, NULL, &rx_chan));

    i2s_std_config_t std_cfg = {
        .clk_cfg  = I2S_STD_CLK_DEFAULT_CONFIG(I2S_AUDIO_RATE),
        .slot_cfg = I2S_STD_PHILIPS_SLOT_DEFAULT_CONFIG(I2S_DATA_BIT_WIDTH_16BIT, I2S_SLOT_MODE_MONO),
        .gpio_cfg = {
            .mclk = I2S_GPIO_UNUSED,
            .bclk = I2S_BCK_PIN,
            .ws   = I2S_WS_PIN,
            .dout = I2S_GPIO_UNUSED,
            .din  = I2S_DIN_PIN
        }
    };
    std_cfg.clk_cfg.mclk_multiple = I2S_MCLK_MULTIPLE_256;
    std_cfg.slot_cfg.slot_mask = I2S_STD_SLOT_LEFT;

    ESP_ERROR_CHECK(i2s_channel_init_std_mode(rx_chan, &std_cfg));
    ESP_ERROR_CHECK(i2s_channel_enable(rx_chan));

    ESP_LOGI(TAG,
        "I2S oversample: logic=%u sps  Fs=%u Hz  DIN=GPIO%d  BCK=%d WS=%d  DMA frame=%u desc=%u",
        (unsigned)LOGIC_SAMPLE_HZ,
        (unsigned)I2S_AUDIO_RATE,
        I2S_DIN_PIN, I2S_BCK_PIN, I2S_WS_PIN,
        (unsigned)I2S_DMA_FRAME_NUM, (unsigned)I2S_DMA_DESC_NUM);
}

// Convert I2S captures to(level, duration_us) runs until an idle gap.
static size_t collect_burst(seg_t *out, size_t cap)
{
    const uint32_t gap_samples =
        (uint32_t)((uint64_t)FRAME_GAP_US * LOGIC_SAMPLE_HZ / 1000000ULL);

    size_t n = 0;
    static int last_lvl = -1;
    uint32_t run = 0;

    static int16_t dma_buf[I2S_DMA_FRAME_NUM];
    size_t bytes_read = 0;

    const TickType_t max_wait = pdMS_TO_TICKS(5);

    int idle_loops   = 0;
    int slice_budget = 0;

    while (n < cap)
    {
        esp_err_t err = i2s_channel_read(rx_chan, dma_buf, sizeof(dma_buf),
                                         &bytes_read, max_wait);
        if (err != ESP_OK || bytes_read == 0) {
            if (++idle_loops > 5) break;
            vTaskDelay(1);
            continue;
        }
        idle_loops = 0;

        if (++slice_budget >= 4) break;

        size_t nsamp16 = bytes_read / sizeof(int16_t);
        for (size_t i = 0; i < nsamp16; i++) {
            uint16_t w = (uint16_t)dma_buf[i];

            // unpack 16 logic samples
            for (int b = 15; b >= 0; --b) {
                int lvl = (w >> b) & 1;

                if (last_lvl < 0) { last_lvl = lvl; run = 1; continue; }

                if (lvl == last_lvl) {
                    run++;
                } else {
                    if (run > 0 && n < cap) {
                        uint32_t dur_us =
                            (uint32_t)((uint64_t)run * 1000000ULL / LOGIC_SAMPLE_HZ);
                        if (dur_us == 0) dur_us = 1;
                        out[n++] = (seg_t){ .level = (uint8_t)last_lvl, .dur_us = dur_us };
                    }
                    last_lvl = lvl;
                    run = 1;
                }

                if (run >= gap_samples && n >= 12) goto flush_and_exit;
            }
        }

        taskYIELD();
    }

flush_and_exit:
    if (run > 0 && n < cap) {
        uint32_t dur_us = (uint32_t)((uint64_t)run * 1000000ULL / LOGIC_SAMPLE_HZ);
        if (dur_us == 0) dur_us = 1;
        out[n++] = (seg_t){ .level = (uint8_t)last_lvl, .dur_us = dur_us };
    }

    return n;
}

//median of early segments to calculate half-bit estimate
static bool estimate_half_us_from_preamble(const seg_t *segs_in, size_t n_segs_in, float *half_us_out)
{
    uint32_t tmp[64]; size_t n = 0;
    for (size_t i = 0; i < n_segs_in && n < 64; i++)
        if (segs_in[i].dur_us) tmp[n++] = segs_in[i].dur_us;
    if (n < 16) return false;
    for (size_t i = 1; i < n; i++) {
        uint32_t k = tmp[i]; size_t j = i;
        while (j > 0 && tmp[j - 1] > k) { tmp[j] = tmp[j - 1]; j--; }
        tmp[j] = k;
    }
    uint32_t med = tmp[n / 2];
    if (med < 5 || med > 50000) return false;
    *half_us_out = (float)med;
    return true;
}

//segments to half-bit level samples (nearest integer; reject tiny glitches)
static size_t segs_to_half_levels(const seg_t *segs_in, size_t n_segs_in, float half_us, uint8_t *halves_out, size_t cap)
{
    size_t n = 0;
    for (size_t i = 0; i < n_segs_in; i++) {
        float r = (float)segs_in[i].dur_us / half_us;
        if (r < 0.75f) continue; // glitch reject
        int cnt = (int)(r + 0.5f);
        if (cnt < 1) cnt = 1;
        if (cnt > 8) cnt = 8;
        uint8_t lvl = segs_in[i].level ? 1 : 0;
        for (int k = 0; k < cnt && n < cap; k++) halves_out[n++] = lvl;
        if (n >= cap) break;
    }
    return n;
}

//Manchester IEEE 1 = low to high, 0 = high to low
static size_t halves_to_bits_manchester(const uint8_t *halves_in, size_t n_halves_in,
                                        uint8_t *bits_out, size_t bit_cap, int invert)
{
    size_t i = 0, nb = 0;
    while (i + 1 < n_halves_in && nb < bit_cap) {
        uint8_t a = halves_in[i] ^ (invert ? 1 : 0);
        uint8_t b = halves_in[i + 1] ^ (invert ? 1 : 0);
        if      (a == 0 && b == 1) { bits_out[nb++] = 1; i += 2; }
        else if (a == 1 && b == 0) { bits_out[nb++] = 0; i += 2; }
        else { i += 1; }
    }
    return nb;
}

//pack bits into bytes starting at bit offset `off` (0..7), MSB-first
static size_t bits_to_bytes_msb_off(const uint8_t *bits_in, size_t n_bits_in, int off,
                                    uint8_t *bytes_out, size_t byte_cap)
{
    if (off < 0) off = 0;
    if (off > 7) off = 7;
    if (n_bits_in <= (size_t)off) return 0;
    size_t usable = n_bits_in - (size_t)off;
    size_t n = usable / 8;
    if (n > byte_cap) n = byte_cap;
    size_t bi = (size_t)off;
    for (size_t i = 0; i < n; i++) {
        uint8_t b = 0;
        for (int k = 0; k < 8; k++, bi++) b = (b << 1) | (bits_in[bi] & 1);
        bytes_out[i] = b;
    }
    return n;
}

static bool find_frame_bounds(const uint8_t *bytes_in, size_t n, size_t *s, size_t *e)
{
    for (size_t i = 0; i + 2 < n; i++) {
        if (bytes_in[i] == 0xAA) {
            for (size_t j = i + 2; j < n; j++)
                if (bytes_in[j] == 0x55) { *s = i; *e = j; return true; }
        }
    }
    return false;
}

static size_t deescape_between_markers(const uint8_t *bytes_in, size_t s, size_t e, uint8_t *out, size_t cap)
{
    size_t n = 0;
    for (size_t i = s + 1; i < e; i++) {
        uint8_t b = bytes_in[i];
        if (b == 0x7D) {
            if (i + 1 >= e) return 0;
            uint8_t x = bytes_in[++i] ^ 0x20;
            if (n < cap) out[n++] = x; else return 0;
        } else {
            if (n < cap) out[n++] = b; else return 0;
        }
    }
    return n;
}

// ===================== Alignment logic =====================
static const uint8_t ALIGN_PAYLOAD[4] = {0xC3, 0x5A, 0xA5, 0x3C};

#define ALIGN_REQUIRED_GOOD   2
#define ALIGN_WINDOW_US       300000

typedef struct {
    bool aligned;
    int  good_aligns;
    int64_t first_align_us;
    int64_t last_seen_us;
    uint8_t last_align_seq;
} align_state_t;

static align_state_t g_align = (align_state_t){0};

static inline void align_reset(void)
{
    g_align.aligned = false;
    g_align.good_aligns = 0;
    g_align.first_align_us = 0;
    gpio_set_level(LED_GRN_GPIO, 0);

    seq_locked   = false;
    expected_seq = 0;
    last_delivered_seq = 0xFF;

    static const uint8_t zero[8] = {0};
    memcpy(last_kbd, zero, 8);

    // send "all up"
    if (tud_hid_ready()) {
        tud_hid_report(0, zero, 8);
    } else {
        memcpy(pending_kbd, zero, 8);
        has_pending_kbd = true;
    }
}

static inline void align_note_valid_frame(bool is_align, uint8_t align_seq_if_any)
{
    int64_t now = esp_timer_get_time();
    g_align.last_seen_us = now;

    if (!g_align.aligned && is_align) {
        g_align.last_align_seq = align_seq_if_any;

        if (g_align.first_align_us == 0) {
            g_align.first_align_us = now; g_align.good_aligns = 1;
        } else {
            if (now - g_align.first_align_us <= ALIGN_WINDOW_US) {
                g_align.good_aligns++;
            } else {
                g_align.first_align_us = now; g_align.good_aligns = 1;
            }
        }
        if (g_align.good_aligns >= ALIGN_REQUIRED_GOOD) {
            g_align.aligned = true;
            gpio_set_level(LED_GRN_GPIO, 1); //green on = aligned
            ESP_LOGI(TAG, "ALIGN LOCKED — sending minimal ACK (seq=%u)", (unsigned)g_align.last_align_seq);

            seq_locked   = false;
            expected_seq = 0;
            last_delivered_seq = 0xFF;

            for (int i = 0; i < 10; i++) {
            send_ack_frame(g_align.last_align_seq, "A:ACK");
            }
        }
    }
}

// ===================== USB HID (Keyboard + Mouse) =====================
enum {
    ITF_NUM_HID_KBD = 0,
    ITF_NUM_HID_MOUSE,
    ITF_NUM_TOTAL
};

#define EPNUM_HID_KBD   0x81
#define EPNUM_HID_MOUSE 0x82

static const uint8_t hid_report_desc_kbd[]   = { TUD_HID_REPORT_DESC_KEYBOARD() };
static const uint8_t hid_report_desc_mouse[] = { TUD_HID_REPORT_DESC_MOUSE() };

static const tusb_desc_device_t desc_device = {
  .bLength            = sizeof(tusb_desc_device_t),
  .bDescriptorType    = TUSB_DESC_DEVICE,
  .bcdUSB             = 0x0200,
  .bDeviceClass       = 0x00,
  .bDeviceSubClass    = 0x00,
  .bDeviceProtocol    = 0x00,
  .bMaxPacketSize0    = CFG_TUD_ENDPOINT0_SIZE,
  .idVendor           = 0x303A,
  .idProduct          = 0x4003,
  .bcdDevice          = 0x0100,
  .iManufacturer      = 0x01,
  .iProduct           = 0x02,
  .iSerialNumber      = 0x03,
  .bNumConfigurations = 0x01
};

static const char*  string_desc[] = {
    (const char[]){ 0x09, 0x04 },
    "FSOCSKYBM",
    "ESP32S3 HID Keyboard+Mouse",
    "123456"
};

#define CONFIG_TOTAL_LEN (TUD_CONFIG_DESC_LEN + TUD_HID_DESC_LEN + TUD_HID_DESC_LEN)
static const uint8_t desc_configuration[] = {
    TUD_CONFIG_DESCRIPTOR(1, ITF_NUM_TOTAL, 0, CONFIG_TOTAL_LEN,
                          TUSB_DESC_CONFIG_ATT_REMOTE_WAKEUP, 100),

    // Keyboard
    TUD_HID_DESCRIPTOR(ITF_NUM_HID_KBD,
                       0,
                       HID_ITF_PROTOCOL_KEYBOARD,
                       sizeof(hid_report_desc_kbd),
                       EPNUM_HID_KBD,
                       8,
                       10),

    // Mouse
    TUD_HID_DESCRIPTOR(ITF_NUM_HID_MOUSE,
                       0,
                       HID_ITF_PROTOCOL_NONE,
                       sizeof(hid_report_desc_mouse),
                       EPNUM_HID_MOUSE,
                       8,
                       1)
};

static void usb_hid_init(void)
{
    tinyusb_config_t tusb_cfg = {
        .device_descriptor        = &desc_device,
        .configuration_descriptor = desc_configuration,
        .string_descriptor        = string_desc,
        .string_descriptor_count  = sizeof(string_desc) / sizeof(string_desc[0]),
        .external_phy             = false
    };

    ESP_ERROR_CHECK(tinyusb_driver_install(&tusb_cfg));
    ESP_LOGI(TAG, "USB HID keyboard+mouse ready");
}

//Required HID callbacks
uint16_t tud_hid_get_report_cb(uint8_t itf,
                               uint8_t report_id,
                               hid_report_type_t report_type,
                               uint8_t* buffer,
                               uint16_t reqlen)
{
    (void)itf; (void)report_id; (void)report_type; (void)buffer; (void)reqlen;
    return 0;
}
void tud_hid_set_report_cb(uint8_t itf,
                           uint8_t report_id,
                           hid_report_type_t report_type,
                           uint8_t const* buffer,
                           uint16_t bufsize)
{
    (void)itf; (void)report_id; (void)report_type; (void)buffer; (void)bufsize;
}

void tud_mount_cb(void)
{
    static const uint8_t z[8]={0};
    if (tud_hid_n_ready(ITF_NUM_HID_KBD)) {
        tud_hid_n_report(ITF_NUM_HID_KBD,0,z,8);
    } else {
        memcpy(pending_kbd,z,8);
        has_pending_kbd=true;
    }
}
void tud_umount_cb(void)         { }
void tud_suspend_cb(bool rwup_en)
{
    (void)rwup_en;
    static const uint8_t z[8]={0};
    if (tud_hid_n_ready(ITF_NUM_HID_KBD)) {
        tud_hid_n_report(ITF_NUM_HID_KBD,0,z,8);
    } else {
        memcpy(pending_kbd,z,8);
        has_pending_kbd=true;
    }
}
void tud_resume_cb(void)
{
    static const uint8_t z[8]={0};
    if (tud_hid_n_ready(ITF_NUM_HID_KBD)) {
        tud_hid_n_report(ITF_NUM_HID_KBD,0,z,8);
    } else {
        memcpy(pending_kbd,z,8);
        has_pending_kbd=true;
    }
}

uint8_t const *tud_hid_descriptor_report_cb(uint8_t instance)
{
    if (instance == ITF_NUM_HID_KBD)   return hid_report_desc_kbd;
    if (instance == ITF_NUM_HID_MOUSE) return hid_report_desc_mouse;
    return NULL;
}

// ----------------------- HID send helpers ----------------------
static inline bool hid_try_send_and_maybe_queue(const uint8_t *rep8)
{
    if (tud_hid_n_ready(ITF_NUM_HID_KBD)) {
        tud_hid_n_report(ITF_NUM_HID_KBD, 0, rep8, 8);
        memcpy(last_kbd, rep8, 8);
        last_kbd_tx_us = esp_timer_get_time();
        has_pending_kbd = false;
        led_pulse_kbd();
        return true;
    } else {
        memcpy(pending_kbd, rep8, 8);
        has_pending_kbd = true;
        return false;
    }
}
static inline void hid_send_all_up(void)
{
    static const uint8_t zero[8] = {0};
    (void)hid_try_send_and_maybe_queue(zero);
}
static inline void kbd_idle_maintain(void)
{
    if (has_pending_kbd && tud_hid_n_ready(ITF_NUM_HID_KBD)) {
        tud_hid_n_report(ITF_NUM_HID_KBD, 0, pending_kbd, 8);
        memcpy(last_kbd, pending_kbd, 8);
        last_kbd_tx_us = esp_timer_get_time();
        has_pending_kbd = false;
        led_pulse_kbd();
    }
    if (!is_all_zero8(last_kbd) && last_kbd_tx_us) {
        int64_t now = esp_timer_get_time();
        if (now - last_kbd_tx_us > 5000000) {
            static const uint8_t zero[8] = {0};
            (void)hid_try_send_and_maybe_queue(zero);
        }
    }
}

static inline void mouse_send_report(uint8_t buttons, int8_t dx, int8_t dy,
                                     int8_t wheelV, int8_t wheelH_unused)
{
#if DEBUG_RX
    ESP_LOGD(TAG, "MOUSE send: btn=%02X dx=%d dy=%d wheel=%d", buttons, dx, dy, wheelV);
#endif

    if (tud_hid_n_ready(ITF_NUM_HID_MOUSE)) {
        tud_hid_n_mouse_report(ITF_NUM_HID_MOUSE,
                               0 /*report ID*/,
                               buttons,
                               dx,
                               dy,
                               wheelV,
                               0 /* horizontal pan */);
        led_pulse_activity_us(12000);
    }
}

// ------------------------ RX delivery & ACK policy -------------------
static inline void rx_handle_keyboard_frame(uint8_t seq, const uint8_t *payload)
{
    bool is_zero = is_all_zero8(payload);

    if (!seq_locked) {
        if (memcmp(last_kbd, payload, 8) != 0) {
            memcpy(last_kbd, payload, 8);
            hid_try_send_and_maybe_queue(payload);
        }
        last_delivered_seq = seq;
        expected_seq       = (uint8_t)(seq + 1);
        seq_locked         = true;
        return;
    }

    if (seq == expected_seq) {
        if (memcmp(last_kbd, payload, 8) != 0) {
            memcpy(last_kbd, payload, 8);
            hid_try_send_and_maybe_queue(payload);
        }
        last_delivered_seq = seq;
        expected_seq++;
        send_ack_frame(seq, "A:inorder");
        return;
    }

    if (seq == last_delivered_seq) {
        send_ack_frame(last_delivered_seq, "A:dup");
        return;
    }

    send_ack_frame(last_delivered_seq, "A:re-ack");

    if (is_zero && memcmp(last_kbd, payload, 8) != 0) {
        memcpy(last_kbd, payload, 8);
        hid_try_send_and_maybe_queue(payload);
    }
}

static inline void rx_handle_mouse_frame(uint8_t seq, const uint8_t *pl)
{
    uint8_t buttons = pl[0];
    int8_t  dx      = (int8_t)pl[1];
    int8_t  dy      = (int8_t)pl[2];
    int8_t  whV     = (int8_t)pl[3];
    int8_t  whH     = (int8_t)pl[4];

    if (!seq_locked) {
        mouse_send_report(buttons, dx, dy, whV, whH);
        last_delivered_seq = seq;
        expected_seq       = (uint8_t)(seq + 1);
        seq_locked         = true;
        return;
    }

    if (seq == expected_seq) {
        mouse_send_report(buttons, dx, dy, whV, whH);
        last_delivered_seq = seq;
        expected_seq++;
        send_ack_frame(seq, "A:inorder");
        return;
    }

    if (seq == last_delivered_seq) {
        send_ack_frame(last_delivered_seq, "A:dup");
        return;
    }

    send_ack_frame(last_delivered_seq, "A:re-ack");
}

// ===================== RX task =====================
static void *alloc8_pref_psram(size_t bytes)
{
    void *p = heap_caps_malloc(bytes, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!p) p = heap_caps_malloc(bytes, MALLOC_CAP_8BIT);
    return p;
}

// forward decls already above, so no changes here

static void rx_loop_task(void *arg)
{
    uint32_t fail_count = 0;

    for (;;)
    {
        leds_maintain();
        kbd_idle_maintain();

        size_t n_segs = collect_burst(segs, MAX_SEGS);
        if (n_segs < 16) { vTaskDelay(pdMS_TO_TICKS(2)); continue; }

        float half_us = 0.f;
        bool have_est = estimate_half_us_from_preamble(segs, n_segs, &half_us);
        const float min_half = (float)HALF_GUESS_US * 0.5f;
        const float max_half = (float)HALF_GUESS_US * 1.5f;
        if (!have_est || half_us < min_half || half_us > max_half) {
            half_us = (float)HALF_GUESS_US;
#if DEBUG_RX
            ESP_LOGD(TAG, "half-est: fallback to guess %.1fus (est_ok=%d)", half_us, (int)have_est);
#endif
        } else {
#if DEBUG_RX
            ESP_LOGD(TAG, "half-est: %.1fus (ok)", half_us);
#endif
        }

        size_t n_halves = segs_to_half_levels(segs, n_segs, half_us, halves, MAX_HALVES);
        if (n_halves < 32) {
#if DEBUG_RX
            if ((fail_count++ % DEBUG_RATE_LIMIT) == 0) {
                ESP_LOGW(TAG, "FAIL: too few halves=%u (from segs=%u)", (unsigned)n_halves, (unsigned)n_segs);
                size_t show = (n_halves < DEBUG_DUMP_HALVES) ? n_halves : DEBUG_DUMP_HALVES;
                printf("Halves: "); dump_halves_line(halves, show);
            }
#endif
            vTaskDelay(pdMS_TO_TICKS(2));
            continue;
        }

        int used_invert = -1;
        int found_off   = -1;
        size_t n_bits   = 0, n_bytes = 0, s = 0, e = 0;

        for (int inv = 0; inv <= 1 && found_off < 0; inv++) {
            n_bits = halves_to_bits_manchester(halves, n_halves, bits, MAX_BITS, inv);
            if (n_bits < 64) {
#if DEBUG_RX
                if ((fail_count++ % DEBUG_RATE_LIMIT) == 0) {
                    ESP_LOGW(TAG, "FAIL: too few bits=%u (inv=%d)", (unsigned)n_bits, inv);
                    size_t showb = (n_bits < DEBUG_DUMP_BITS) ? n_bits : DEBUG_DUMP_BITS;
                    printf("Bits:   "); dump_bits_line(bits, showb);
                }
#endif
                continue;
            }
            for (int off = 0; off < 8 && found_off < 0; off++) {
                n_bytes = bits_to_bytes_msb_off(bits, n_bits, off, bytes, MAX_BYTES);
                if (n_bytes < 8) continue;
                if (find_frame_bounds(bytes, n_bytes, &s, &e)) {
                    used_invert = inv;
                    found_off = off;
                }
            }
        }

        if (found_off < 0) {
#if DEBUG_RX
            if ((fail_count++ % DEBUG_RATE_LIMIT) == 0) {
                ESP_LOGW(TAG, "FAIL: no AA..55 frame (halves=%u bits=%u)", (unsigned)n_halves, (unsigned)n_bits);
                size_t nb = (n_bytes < DEBUG_DUMP_BYTES) ? n_bytes : DEBUG_DUMP_BYTES;
                printf("Bytes(off=0) HEX: "); dump_hex_bytes(bytes, nb);
                printf("Bytes(off=0) BIN: "); dump_bin_bytes_line(bytes, nb);
            }
#endif
            vTaskDelay(pdMS_TO_TICKS(2));
            continue;
        }

        size_t fr_len = deescape_between_markers(bytes, s, e, frame_buf, MAX_BYTES);
        if (fr_len < 3 + 4) {
#if DEBUG_RX
            if ((fail_count++ % DEBUG_RATE_LIMIT) == 0) {
                ESP_LOGW(TAG, "FAIL: frame too short deescaped=%u (s=%u e=%u)", (unsigned)fr_len, (unsigned)s, (unsigned)e);
            }
#endif
            vTaskDelay(1);
            continue;
        }

        uint8_t type = frame_buf[0];
        uint8_t seq  = frame_buf[1];
        uint8_t plen = frame_buf[2];

        size_t needed = 3 + (size_t)plen + 4;
        if (fr_len < needed) {
#if DEBUG_RX
            if ((fail_count++ % DEBUG_RATE_LIMIT) == 0) {
                ESP_LOGW(TAG, "FAIL: frame_len=%u < needed=%u (plen=%u)", (unsigned)fr_len, (unsigned)needed, (unsigned)plen);
            }
#endif
            vTaskDelay(1);
            continue;
        }

        const uint8_t *payload = &frame_buf[3];
        const uint8_t *crc_ptr = &payload[plen];
        uint32_t crc_rx = u32_le(crc_ptr);
        uint32_t crc_calc = crc32_update(0, frame_buf, 3 + plen);
        if (crc_calc != crc_rx) {
#if DEBUG_RX
            if ((fail_count++ % DEBUG_RATE_LIMIT) == 0) {
                ESP_LOGW(TAG, "FAIL: CRC mismatch calc=%08X rx=%08X (seq=%u type=0x%02X inv=%d off=%d)",
                         (unsigned)crc_calc, (unsigned)crc_rx, (unsigned)seq, (unsigned)type, used_invert, found_off);
            }
#endif
            vTaskDelay(1);
            continue;
        }

        // Success path
        led_pulse_activity_us(LED_ACTIVITY_US);
        bool is_align = (type == TYPE_ALIGN && plen >= sizeof(ALIGN_PAYLOAD) &&
                         memcmp(payload, ALIGN_PAYLOAD, sizeof(ALIGN_PAYLOAD)) == 0);
        align_note_valid_frame(is_align, seq);

#if DEBUG_RX
        ESP_LOGI(TAG, "OK: half≈%.1fus inv=%d off=%d type=0x%02X(%c) seq=%u len=%u",
                 half_us, used_invert, found_off, type,
                 (type >= 32 && type < 127) ? type : '.', (unsigned)seq, (unsigned)plen);
#endif

        if (type == TYPE_KBD && plen == 8) {
            rx_handle_keyboard_frame(seq, payload);
        } else if (type == TYPE_MOUSE && plen == 5) {
            rx_handle_mouse_frame(seq, payload);
        }

        vTaskDelay(pdMS_TO_TICKS(1));
    }
}

// ---------------- app_main ----------------
static void init_gpio_and_buffers(void)
{
    // LEDs first
    leds_init();
    gpio_set_level(LED_GRN_GPIO, 0);

    // Laser enable GPIO
    laser_gpio_init();

    // ACK TX line idle low
    gpio_config_t txio = {
        .pin_bit_mask = 1ULL << TX_GPIO,
        .mode = GPIO_MODE_OUTPUT,
        .pull_up_en = GPIO_PULLUP_DISABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type = GPIO_INTR_DISABLE
    };
    ESP_ERROR_CHECK(gpio_config(&txio));
    gpio_set_level(TX_GPIO, 0);

    // Allocate large buffers
    segs      = (seg_t   *)alloc8_pref_psram(sizeof(seg_t)   * MAX_SEGS);
    halves    = (uint8_t *)alloc8_pref_psram(sizeof(uint8_t) * MAX_HALVES);
    bits      = (uint8_t *)alloc8_pref_psram(sizeof(uint8_t) * MAX_BITS);
    bytes     = (uint8_t *)alloc8_pref_psram(sizeof(uint8_t) * MAX_BYTES);
    frame_buf = (uint8_t *)alloc8_pref_psram(sizeof(uint8_t) * MAX_BYTES);

    if (!segs || !halves || !bits || !bytes || !frame_buf) {
        ESP_LOGE(TAG, "Buffer allocation failed (enable PSRAM or reduce caps)");
        fatal_led_on_now(); abort();
    }

    // I2S oversampling init
    i2s_rx_init();
    ESP_LOGI(TAG, "Manchester (I2S oversample) RX DIN=GPIO%d  DATA_RATE=%u bps  OS×=%u  LogicSamp=%u/s  GAP≈%u us",
             RX_GPIO, (unsigned)DATA_RATE_BPS, (unsigned)OS_BITS_PER_DATA,
             (unsigned)(DATA_RATE_BPS * OS_BITS_PER_DATA), (unsigned)FRAME_GAP_US);
}

void app_main(void)
{
    init_gpio_and_buffers();

    // I2C bus init:
    if (i2c_master_init_simple() != ESP_OK) {
        ESP_LOGE(TAG, "I2C init failed - cannot talk to sensor bus");
        fatal_led_on_now();
        abort();
    }

    // init INA219.
    esp_err_t ina_init_err = ina219_init();
    if (ina_init_err == ESP_OK) {
        g_current_monitor_present = ina219_probe();
    } else {
        ESP_LOGW(TAG, "INA219 init failed (%s). Assuming no current monitor.", esp_err_to_name(ina_init_err));
        g_current_monitor_present = false;
    }

    if (g_current_monitor_present) {
        // Start laser guard (Core 0).
        xTaskCreatePinnedToCore(laser_guard_task, "laser_guard", 2048, NULL, 5, NULL, 0);
    } else {
        ESP_LOGW(TAG, "Laser guard DISABLED (no INA219). Laser will stay enabled without HW current trip.");
        // Make sure YELLOW is off so operator doesn't think it's faulted.
        gpio_set_level(LED_YEL_GPIO, 0);
        // keep laser enabled (hardware pin) and allow modulation
        laser_enable(true);
        g_tx_allowed = true;
    }

    // USB HID
    usb_hid_init();

    // Ensure host starts with a clean keyboard state
    hid_send_all_up();

    // Start RX worker on Core 1
    BaseType_t ok = xTaskCreatePinnedToCore(rx_loop_task, "rx_loop", 8192, NULL, 4, NULL, 1);
    if (ok != pdPASS) { ESP_LOGE(TAG, "rx_loop task create failed"); fatal_led_on_now(); abort(); }

    // app_main should not block so Idle0 can always run the WDT
    vTaskDelete(NULL);
}
