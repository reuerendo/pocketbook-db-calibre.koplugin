# calibre.koplugin for Pocketbook devices
Module for the plugin calibre.koplugin to the KOreader application. The program adds information about books sent via the Smart Device App protocol of the Calibre application to the PocketBook device database. This eliminates the need to reboot the device to find books in the reader's built-in library.

## Installation:
Add files [pb-dp.lua](calibre.koplugin/pb-db.lua), [main.lua](calibre.koplugin/main.lua), [metadata.lua](calibre.koplugin/metadata.lua) and [wireless.lua](calibre.koplugin/wireless.lua) to the folder `/applications/koreader/plugins/calibre.koplugin`
The original files should be replaced.

## Collections
The plugin can add books to collections in the PocketBook library and to the KOreader collections. To use this function, you should specify the column lookup name in the calibre (in the #name format) in the SmartDevice App Interface settings.

<img src="/col.png" width="445">

## Read status and Favorite
The module also supports read status and favorite marking. In the settings, you need to specify the lookup name for the "Read" and "Favorite" columns. Both columns should be of the yes/no type.

The program can transmit the status and date of reading to Calibre when connected. Please make a backup copy of your Calibre database[^1].
[^1]: The program was tested on a PocketBook InkPad 4 and Touch Lux 5 devices.

*The code was mostly written by Claude AI. A huge thanks to [Volodymyr Streltsov](https://github.com/VolodymyrStreltsov), who explained to me and Claude what bind and bind1 are.*

## connect-to-calibre.app
You can also try a [standalone app](https://github.com/reuerendo/connect-to-calibre.app) with similar functionality. It runs significantly faster thanks to metadata caching.
