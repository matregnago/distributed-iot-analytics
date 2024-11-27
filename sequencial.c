#define _XOPEN_SOURCE 700
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    double timestamp; // seconds since epoch
    double value;     // reading value
} Reading;

typedef struct {
    char *device_name;
    Reading *temp_readings;
    Reading *humidity_readings;
    Reading *luminosity_readings;
    int num_temp_readings;
    int num_humidity_readings;
    int num_luminosity_readings;
    int max_temp_readings;
    int max_humidity_readings;
    int max_luminosity_readings;
} DeviceReadings;

typedef struct {
    const char *device_name;
    double start_time;
    double end_time;
    double value;
    double duration;
} Interval;

DeviceReadings *devices = NULL;
int num_devices = 0;
int max_devices = 0;

DeviceReadings* get_device_readings(const char *device_name) {
    for (int i = 0; i < num_devices; i++) {
        if (strcmp(devices[i].device_name, device_name) == 0) {
            return &devices[i];
        }
    }
    // Add new device
    if (num_devices == max_devices) {
        max_devices = max_devices == 0 ? 10 : max_devices * 2;
        devices = realloc(devices, max_devices * sizeof(DeviceReadings));
        if (devices == NULL) {
            perror("Error allocating memory for devices");
            exit(EXIT_FAILURE);
        }
    }
    devices[num_devices].device_name = strdup(device_name);
    devices[num_devices].temp_readings = NULL;
    devices[num_devices].humidity_readings = NULL;
    devices[num_devices].luminosity_readings = NULL;
    devices[num_devices].num_temp_readings = 0;
    devices[num_devices].num_humidity_readings = 0;
    devices[num_devices].num_luminosity_readings = 0;
    devices[num_devices].max_temp_readings = 0;
    devices[num_devices].max_humidity_readings = 0;
    devices[num_devices].max_luminosity_readings = 0;
    num_devices++;
    return &devices[num_devices - 1];
}

double parse_timestamp(const char *date_str) {
    struct tm tm = {0};
    char *res = strptime(date_str, "%Y-%m-%d %H:%M:%S", &tm);
    if (res == NULL) {
        fprintf(stderr, "Failed to parse date: %s\n", date_str);
        return -1;
    }
    double fractional_seconds = 0.0;
    if (*res == '.') {
        fractional_seconds = atof(res);
    }
    time_t time_epoch = mktime(&tm);
    return (double)time_epoch + fractional_seconds;
}

void format_timestamp(double timestamp, char *buffer, size_t size) {
    time_t t = (time_t)timestamp;
    struct tm *tm_info = localtime(&t);
    strftime(buffer, size, "%Y-%m-%d %H:%M:%S", tm_info);
    int milliseconds = (int)((timestamp - t) * 1000);
    char ms_buffer[8];
    sprintf(ms_buffer, ".%03d", milliseconds);
    strcat(buffer, ms_buffer);
}

int compare_intervals(const void *a, const void *b) {
    const Interval *ia = (const Interval *)a;
    const Interval *ib = (const Interval *)b;
    if (ia->duration < ib->duration) return 1;
    else if (ia->duration > ib->duration) return -1;
    else return 0;
}

void process_intervals(Reading *readings, int num_readings, Interval **intervals, int *num_intervals, int *max_intervals, const char *device_name) {
    if (num_readings == 0) return;

    double prev_value = readings[0].value;
    double start_time = readings[0].timestamp;
    for (int j = 1; j < num_readings; j++) {
        if (readings[j].value != prev_value) {
            double end_time = readings[j - 1].timestamp;
            double duration = end_time - start_time;
            if (duration > 0) {
                if (*num_intervals == *max_intervals) {
                    *max_intervals = *max_intervals == 0 ? 100 : *max_intervals * 2;
                    *intervals = realloc(*intervals, (*max_intervals) * sizeof(Interval));
                    if (*intervals == NULL) {
                        perror("Error allocating memory for intervals");
                        exit(EXIT_FAILURE);
                    }
                }
                Interval interval;
                interval.device_name = device_name;
                interval.start_time = start_time;
                interval.end_time = end_time;
                interval.value = prev_value;
                interval.duration = duration;
                (*intervals)[(*num_intervals)++] = interval;
            }
            prev_value = readings[j].value;
            start_time = readings[j].timestamp;
        }
    }
    // Handle the last interval
    double end_time = readings[num_readings - 1].timestamp;
    double duration = end_time - start_time;
    if (duration > 0) {
        if (*num_intervals == *max_intervals) {
            *max_intervals = *max_intervals == 0 ? 100 : *max_intervals * 2;
            *intervals = realloc(*intervals, (*max_intervals) * sizeof(Interval));
            if (*intervals == NULL) {
                perror("Error allocating memory for intervals");
                exit(EXIT_FAILURE);
            }
        }
        Interval interval;
        interval.device_name = device_name;
        interval.start_time = start_time;
        interval.end_time = end_time;
        interval.value = prev_value;
        interval.duration = duration;
        (*intervals)[(*num_intervals)++] = interval;
    }
}

int main(int argc, char *argv[]) {

    if (argc < 2) {
        printf("Usage: %s <input_file>\n", argv[0]);
        return 1;
    }
    char *filename = argv[1];

    FILE *fp = fopen(filename, "r");
    if (fp == NULL) {
        perror("Failed to open the file");
        return 1;
    }

    char line[1024];
    // Skip header line
    fgets(line, sizeof(line), fp);

    while (fgets(line, sizeof(line), fp) != NULL) {
        // Parse the line
        char *token;
        char *rest = line;
        int field = 0;
        char *id = NULL, *device = NULL, *date_str = NULL;
        char *temp_str = NULL, *humidity_str = NULL, *luminosity_str = NULL;
        while ((token = strtok_r(rest, "|", &rest))) {
            switch (field) {
                case 0: id = token; break;
                case 1: device = token; break;
                case 3: date_str = token; break;
                case 4: temp_str = token; break;
                case 5: humidity_str = token; break;
                case 6: luminosity_str = token; break;
            }
            field++;
        }
        if (device == NULL || date_str == NULL || strlen(date_str) < 19) {
            continue; // Missing data
        }
        double timestamp = parse_timestamp(date_str);
        if (timestamp < 0) {
            continue;
        }
        DeviceReadings *dr = get_device_readings(device);

        // Process temperature
        if (temp_str != NULL && strlen(temp_str) > 0) {
            double temp_value = atof(temp_str);
            if (dr->num_temp_readings == dr->max_temp_readings) {
                dr->max_temp_readings = dr->max_temp_readings == 0 ? 100 : dr->max_temp_readings * 2;
                dr->temp_readings = realloc(dr->temp_readings, dr->max_temp_readings * sizeof(Reading));
                if (dr->temp_readings == NULL) {
                    perror("Error allocating memory for temp_readings");
                    exit(EXIT_FAILURE);
                }
            }
            dr->temp_readings[dr->num_temp_readings].timestamp = timestamp;
            dr->temp_readings[dr->num_temp_readings].value = temp_value;
            dr->num_temp_readings++;
        }

        // Process humidity
        if (humidity_str != NULL && strlen(humidity_str) > 0) {
            double humidity_value = atof(humidity_str);
            if (dr->num_humidity_readings == dr->max_humidity_readings) {
                dr->max_humidity_readings = dr->max_humidity_readings == 0 ? 100 : dr->max_humidity_readings * 2;
                dr->humidity_readings = realloc(dr->humidity_readings, dr->max_humidity_readings * sizeof(Reading));
                if (dr->humidity_readings == NULL) {
                    perror("Error allocating memory for humidity_readings");
                    exit(EXIT_FAILURE);
                }
            }
            dr->humidity_readings[dr->num_humidity_readings].timestamp = timestamp;
            dr->humidity_readings[dr->num_humidity_readings].value = humidity_value;
            dr->num_humidity_readings++;
        }

        // Process luminosity
        if (luminosity_str != NULL && strlen(luminosity_str) > 0) {
            double luminosity_value = atof(luminosity_str);
            if (dr->num_luminosity_readings == dr->max_luminosity_readings) {
                dr->max_luminosity_readings = dr->max_luminosity_readings == 0 ? 100 : dr->max_luminosity_readings * 2;
                dr->luminosity_readings = realloc(dr->luminosity_readings, dr->max_luminosity_readings * sizeof(Reading));
                if (dr->luminosity_readings == NULL) {
                    perror("Error allocating memory for luminosity_readings");
                    exit(EXIT_FAILURE);
                }
            }
            dr->luminosity_readings[dr->num_luminosity_readings].timestamp = timestamp;
            dr->luminosity_readings[dr->num_luminosity_readings].value = luminosity_value;
            dr->num_luminosity_readings++;
        }
    }

    fclose(fp);

    // Initialize intervals
    Interval *temp_intervals = NULL;
    Interval *humidity_intervals = NULL;
    Interval *luminosity_intervals = NULL;
    int num_temp_intervals = 0, num_humidity_intervals = 0, num_luminosity_intervals = 0;
    int max_temp_intervals = 0, max_humidity_intervals = 0, max_luminosity_intervals = 0;

    // Process devices sequentially
    for (int i = 0; i < num_devices; i++) {
        DeviceReadings *dr = &devices[i];

        // Process temperature
        process_intervals(dr->temp_readings, dr->num_temp_readings, &temp_intervals, &num_temp_intervals, &max_temp_intervals, dr->device_name);

        // Process humidity
        process_intervals(dr->humidity_readings, dr->num_humidity_readings, &humidity_intervals, &num_humidity_intervals, &max_humidity_intervals, dr->device_name);

        // Process luminosity
        process_intervals(dr->luminosity_readings, dr->num_luminosity_readings, &luminosity_intervals, &num_luminosity_intervals, &max_luminosity_intervals, dr->device_name);
    }

    // Sort intervals by decreasing duration
    qsort(temp_intervals, num_temp_intervals, sizeof(Interval), compare_intervals);
    qsort(humidity_intervals, num_humidity_intervals, sizeof(Interval), compare_intervals);
    qsort(luminosity_intervals, num_luminosity_intervals, sizeof(Interval), compare_intervals);

    // Display top 50 temperature intervals
    int top_n = num_temp_intervals < 50 ? num_temp_intervals : 50;
    printf("\nTop %d maiores intervalos para temperatura:\n", top_n);
    for (int i = 0; i < top_n; i++) {
        Interval *interval = &temp_intervals[i];
        char start_str[64], end_str[64];
        format_timestamp(interval->start_time, start_str, sizeof(start_str));
        format_timestamp(interval->end_time, end_str, sizeof(end_str));

        printf("%d Travamento\n Dispositivo: %s\n Valor: %.2f\n Início: %s\n Fim: %s\n Duração: %.2f segundos\n",
            i+1, interval->device_name, interval->value, start_str, end_str, interval->duration);
    }

    // Display top 50 humidity intervals
    top_n = num_humidity_intervals < 50 ? num_humidity_intervals : 50;
    printf("\nTop %d maiores intervalos para umidade:\n", top_n);
    for (int i = 0; i < top_n; i++) {
        Interval *interval = &humidity_intervals[i];
        char start_str[64], end_str[64];
        format_timestamp(interval->start_time, start_str, sizeof(start_str));
        format_timestamp(interval->end_time, end_str, sizeof(end_str));

        printf("%d Travamento\n Dispositivo: %s\n Valor: %.2f\n Início: %s\n Fim: %s\n Duração: %.2f segundos\n",
            i+1, interval->device_name, interval->value, start_str, end_str, interval->duration);
    }

    // Display top 50 luminosity intervals
    top_n = num_luminosity_intervals < 50 ? num_luminosity_intervals : 50;
    printf("\nTop %d maiores intervalos para luminosidade:\n", top_n);
    for (int i = 0; i < top_n; i++) {
        Interval *interval = &luminosity_intervals[i];
        char start_str[64], end_str[64];
        format_timestamp(interval->start_time, start_str, sizeof(start_str));
        format_timestamp(interval->end_time, end_str, sizeof(end_str));

        printf("%d Travamento\n Dispositivo: %s\n Valor: %.2f\n Início: %s\n Fim: %s\n Duração: %.2f segundos\n",
            i+1, interval->device_name, interval->value, start_str, end_str, interval->duration);
    }

    // Free memory
    for (int i = 0; i < num_devices; i++) {
        free(devices[i].device_name);
        free(devices[i].temp_readings);
        free(devices[i].humidity_readings);
        free(devices[i].luminosity_readings);
    }
    free(devices);
    free(temp_intervals);
    free(humidity_intervals);
    free(luminosity_intervals);

    return 0;
}