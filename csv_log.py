import minimalmodbus
import serial
import time
import csv
import os
from typing import Iterator


def main():
    instrument = init()
    log_filepath = log_file()
    print("Instrument and log file successfully initialized. Logging to CSV file now...")
    
    while True:
        try:
            data_gen = read_pzem_data(instrument)
            write_results(data_gen, log_filepath)
        except KeyboardInterrupt:
            write_results(data_gen, log_filepath)
            instrument.serial.close()
            break


def init():
    """
    Initializes the connection to the PZEM device via a Modbus-to-USB converter
    """
    instrument = minimalmodbus.Instrument('/dev/ttyUSB0', 0x01) # do not name device 0x00, otherwise a problem with minimalmodbus occurs  
    instrument.serial.baudrate = 9600
    instrument.serial.bytesize = 8
    instrument.serial.parity = serial.PARITY_NONE
    instrument.serial.stopbits = 2
    instrument.serial.timeout = 1
    return instrument


def read_pzem_data(instrument) -> Iterator:
    """
    Single read of the measurement data and alarm status from the PZEM device.

    Args:
        instrument: The instrument created in init with minimalmodbus.

    Returns:
        Iterator: A generator object that yields to the read data. 
    
    Raises:
        minimalmodbus.IllegalRequestError: If there is an error accessing the Modbus device.
    """
    try: 
        # Read measurement data:
        voltage = float(instrument.read_register(0x0000, number_of_decimals=2, functioncode=4))
        current = float(instrument.read_register(0x0001, number_of_decimals=2, functioncode=4))
        power_low = instrument.read_register(0x0002, functioncode=4)
        power_high = instrument.read_register(0x0003, functioncode=4)
        power = float((power_high << 16) + power_low)
        energy_low = instrument.read_register(0x0004, functioncode=4)
        energy_high = instrument.read_register(0x0005, functioncode=4)
        energy = float((energy_high << 16) + energy_low)
        
        # Read alarm status
        high_voltage_alarm = instrument.read_register(0x0006, functioncode=4)
        low_voltage_alarm = instrument.read_register(0x0007, functioncode=4)

        # Stack data and timestamp together:
        row_values = [voltage, current, power*0.1, energy, high_voltage_alarm, low_voltage_alarm]
        row_time = time.strftime('%Y-%m-%d %H:%M:%S')
        row_data = [row_time] + row_values
        
    except minimalmodbus.IllegalRequestError as e:
        print(f"Error: {e}")

    finally:
        yield row_data


def log_file(outfile:str='pzem_logs.csv', dir:str='./') -> str:
    """
    Creates a CSV log file within the log_files folder (creates folder if not existing). The CSV file gets initialized with a header row.

    Args:
        outfile (str): The name of the output file. Defaults to 'pzem_logs.csv'.
        dir (str): The dircsv_logectory where the file will be created. Defaults to the current working directory ('./').

    Returns:
        str: The full path to the created log file.

    Raises:
        ValueError: If the `outfile` parameter does not have a `.csv` extension.
        OSError: If there is an error creating the `log_files` folder.
        RuntimeError: If there is an error creating a unique filename to avoid overwriting an existing file.
    """
    os.makedirs(dir, exist_ok=True)

    # Check if the outfile has a .csv extension
    file_name, extension = os.path.splitext(outfile)
    if extension.lower() != '.csv':
        raise ValueError(f"Outfile must be a .csv file, got '{outfile}' instead.")

    # If folder not existent already, create new folder in current working directory:
    new_folder_name = "log_files"
    new_folder_path = os.path.join(dir, new_folder_name)

    try:
        os.makedirs(new_folder_path, exist_ok=True)
    except OSError as e:
        print(f"Error creating folder {new_folder_path}: {e}")

    new_filepath = os.path.join(new_folder_path, outfile)
    
    # Make sure not to overwrite an existing file by adding number to file name, if file exists already
    try:
        n = 1
        while os.path.exists(new_filepath):
            outfile = f"{file_name}_{n}.csv"
            n+=1
            new_filepath = os.path.join(new_folder_path, outfile)
    
    except RuntimeError as e:   
        print(f"Exception occurred: {e}")

    # Write header row:
    header = ["Time", "Voltage in V", "Current in A", "Power in W", "Energy in Wh", "High Voltage Alarm", "Low Voltage Alarm"]
    with open(new_filepath, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)

    return new_filepath


def write_results(csv_strings: Iterator, filepath: str):
    """
    Write a sequence of strings to a file at the specified filepath.

    Args:
        csv_strings (Iterator[str]): An iterator that yields strings to be written to the file.
        filepath (str): The full path to the log_file.
    """
    with open(filepath, 'a', newline='') as f:
        writer = csv.writer(f)
        for csv_str in csv_strings:
            writer.writerow(csv_str)


if __name__ == "__main__":
    main()