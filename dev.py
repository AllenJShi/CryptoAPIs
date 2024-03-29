from Binance import Binance
from Coinbase import Coinbase
from Bittrex import Bittrex
from Gemini import Gemini
from Kraken import Kraken

import PySimpleGUI as sg 
import os.path
import glob
import os

menu_def = [["Help", ["Instruction","About..."]],]

file_list_column = [
    [
        sg.Text("Folder"),
        sg.In(size=(25, 1), enable_events=True, key="-FOLDER-"),
        sg.FolderBrowse(),
    ],
    [
        sg.Listbox(
            values=['Binance','Bittrex','Coinbase','Gemini','Kraken'], enable_events=True, size=(40, 20), key="-FILE LIST-"
        )
    ]
]

# ----- Full layout -----
layout = [
    [
        sg.Column(file_list_column),
        sg.Menu(menu_def,tearoff=False)
    ],
    [sg.Button("Execute All")],
]

window = sg.Window("Organizer", layout)
folder = None
# Run the Event Loop
while True:
    event, values = window.read()
    if event == "Exit" or event == sg.WIN_CLOSED:
        break

    if event == 'About...':
        sg.Popup('About this program','Version 1.1', 'Developed by FLLP')
    elif event == 'Instruction':
        sg.Popup('Please select the destination folder and then left-click on the exchange name to trigger the program.')
    elif event == "-FOLDER-":
        folder = values["-FOLDER-"]
    elif event == "-FILE LIST-" and folder is not None:  # A file was chosen from the listbox
        action = values["-FILE LIST-"][0]
        print("Path: ", folder)
        if action == "Binance":
            print(f"{action} Execution Starts......")
            Binance(path = folder)
            print(f"{action} Execution Completed!")
        
        elif action == "Coinbase":
            print(f"{action} Execution Starts......")
            Coinbase(path = folder)
            print(f"{action} Execution Completed!")            
        
        elif action == "Bittrex":
            print(f"{action} Execution Starts......")
            Bittrex(path=folder)
            print(f"{action} Execution Completed!")  

        elif action == "Gemini":
            print(f"{action} Execution Starts......")
            Gemini(path=folder)
            print(f"{action} Execution Completed!")  

        elif action == "Kraken":
            print(f"{action} Execution Starts......")
            Kraken(path=folder)
            print(f"{action} Execution Completed!")  

        else:
            print("Error")
    elif event == "Execute All" and folder is not None:
        Binance(path=folder)
        Coinbase(path=folder)
        Bittrex(path=folder)
        Gemini(path=folder)
        Kraken(path=folder)

    else:
        print("Folder is None")

window.close()