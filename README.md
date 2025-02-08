# PocketBookDBHandler
Module for the plugin calibre.koplugin to the KOreader application. The program adds information about books sent via the Smart Device App protocol of the Calibre application to the PocketBook device database. This eliminates the need to reboot the device to find books in the reader's built-in library.

## Installation:
Add files pb-dp.lua and wireless.lua to the folder /applications/koreader/plugins/calibre.koplugin.
The original wireless.lua file should be replaced.

## Collections
The plugin adds books to collections on the device. In order to use this feature the collections column lookup name in Calibre should be "#collections".


The program was tested on a PocketBook InkPad 4 device.

The code was mostly written by Claude AI. A huge thanks to [Volodymyr Streltsov](https://github.com/VolodymyrStreltsov), who explained to me and Claude what bind and bind1 are.

