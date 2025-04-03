# calibre.koplugin for Pocketbook devices
Module for the plugin calibre.koplugin to the KOreader application. The program adds information about books sent via the Smart Device App protocol of the Calibre application to the PocketBook device database. This eliminates the need to reboot the device to find books in the reader's built-in library.

## Installation:
Add files [pb-dp.lua](calibre.koplugin/pb-db.lua), [main.lua](calibre.koplugin/main.lua) and [wireless.lua](calibre.koplugin/wireless.lua) to the folder `/applications/koreader/plugins/calibre.koplugin`
The original wireless.lua and main.lua files should be replaced.

## Collections
The plugin adds books to collections on the device. To use this function, you should specify the column lookup name in the calibre (in the #name format) in the plugin settings.

## Read status and Favorite
The module also supports read status and favorite marking. In the settings, you need to specify the lookup name for the "Read" and "Favorite" columns. Both columns should be of the yes/no type.

The program can transmit the status and date of reading to Calibre when connected. Please make a backup copy of your Calibre database[^1].
[^1]: The program was tested on a PocketBook InkPad 4 and Touch Lux 5 devices.

*The code was mostly written by Claude AI. A huge thanks to [Volodymyr Streltsov](https://github.com/VolodymyrStreltsov), who explained to me and Claude what bind and bind1 are.*

