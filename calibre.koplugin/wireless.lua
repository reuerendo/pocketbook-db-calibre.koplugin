--[[
    This module implements the 'smart device app' protocol that communicates with calibre wireless server.
    More details can be found at calibre/devices/smart_device_app/driver.py.
--]]

local BD = require("ui/bidi")
local CalibreExtensions = require("extensions")
local CalibreMetadata = require("metadata")
local CalibreSearch = require("search")
local ConfirmBox = require("ui/widget/confirmbox")
local Device = require("device")
local DocSettings = require("docsettings")
local FFIUtil = require("ffi/util")
local InputDialog = require("ui/widget/inputdialog")
local InfoMessage = require("ui/widget/infomessage")
local NetworkMgr = require("ui/network/manager")
local Trapper = require("ui/trapper")
local UIManager = require("ui/uimanager")
local WidgetContainer = require("ui/widget/container/widgetcontainer")
local lfs = require("libs/libkoreader-lfs")
local logger = require("logger")
local rapidjson = require("rapidjson")
local sha = require("ffi/sha2")
local util = require("util")
local _ = require("gettext")
local T = FFIUtil.template
local ReadCollection = require("readcollection")
local PocketBookDBHandler = require("pb-db")

require("ffi/zeromq_h")

-- calibre broadcast ports used to find calibre server
local BROADCAST_PORTS = {54982, 48123, 39001, 44044, 59678}
-- calibre companion local port
local COMPANION_PORT = 8134
-- requests opcodes
local OPCODES = {
    OK                        = 0,
    SET_CALIBRE_DEVICE_INFO   = 1,
    SET_CALIBRE_DEVICE_NAME   = 2,
    GET_DEVICE_INFORMATION    = 3,
    TOTAL_SPACE               = 4,
    FREE_SPACE                = 5,
    GET_BOOK_COUNT            = 6,
    SEND_BOOKLISTS            = 7,
    SEND_BOOK                 = 8,
    GET_INITIALIZATION_INFO   = 9,
    BOOK_DONE                 = 11,
    NOOP                      = 12,
    DELETE_BOOK               = 13,
    GET_BOOK_FILE_SEGMENT     = 14,
    GET_BOOK_METADATA         = 15,
    SEND_BOOK_METADATA        = 16,
    DISPLAY_MESSAGE           = 17,
    CALIBRE_BUSY              = 18,
    SET_LIBRARY_INFO          = 19,
    ERROR                     = 20,
}

-- Mark some strings for translation.
-- luacheck: push ignore 511
if false then
    -- @translators tcp socket error on host:port
    _("no route to host")
    -- @translators tcp socket error on host:port
    _("host not found")
    -- @translators tcp socket error on host:port
    _("connection refused")
    -- @translators tcp socket error on host:port
    _("timeout")
    -- @translators tcp socket error on host:port
    _("closed")
    -- @translators calibre server address type
    _("discovered")
    -- @translators calibre server address type
    _("specified")
    -- @translators calibre connection error
    _("handshake timeout")
    -- @translators calibre connection error
    _("invalid password")
end
-- luacheck: pop

-- supported formats
local extensions = CalibreExtensions:get()

local function getExtensionPathLengths()
    local t = {}
    for _, v in ipairs(extensions) do
        -- magic number from calibre, see
        -- https://github.com/koreader/koreader/pull/6177#discussion_r430753964
        t[v] = 37
    end
    return t
end

-- get real free space on disk or fallback to 1GB
local function getFreeSpace(dir)
    return util.diskUsage(dir).available or 1024 * 1024 * 1024
end

-- update the view of the dir if we are currently browsing it.
local function updateDir(dir)
    local FileManager = require("apps/filemanager/filemanager")
    local fc = FileManager.instance and FileManager.instance.file_chooser
    if fc and fc.path == dir then
        fc:refreshPath()
    end
end

local CalibreWireless = WidgetContainer:extend{
    id = "KOReader",
    model = require("device").model,
    version = require("version"):getCurrentRevision(),
    calibre = nil, -- hash
}

function CalibreWireless:init()
    self.calibre = {}
end

local function find_calibre_server()
    local socket = require("socket")
    local udp = socket.udp4()
    udp:setoption("broadcast", true)
    udp:setsockname("*", COMPANION_PORT)
    udp:settimeout(3)
    for _, port in ipairs(BROADCAST_PORTS) do
        -- broadcast anything to calibre ports and listen to the reply
        local _, err = udp:sendto("hello", "255.255.255.255", port)
        if not err then
            local dgram, host = udp:receivefrom()
            if dgram and host then
                -- replied diagram has greet message from calibre and calibre hostname
                -- calibre opds port and calibre socket port we will later connect to
                local _, _, replied_port = dgram:match("calibre wireless device client %(on (.-)%);(%d+),(%d+)$")
                udp:close()
                return host, replied_port
            end
        end
    end
    udp:close()
end

local function check_host_port(host, port)
    local socket = require("socket")
    -- luacheck: ignore 311
    local ok, err = socket.dns.getaddrinfo(host)
    if not ok then
        return false, "host not found"
    end
    local ip = ok[1].addr
    local tcp = socket.tcp()
    tcp:settimeout(5)
    -- In case of error, the method returns nil followed by a string
    -- describing the error. In case of success, the method returns 1.
    ok, err = tcp:connect(ip, port)
    tcp:close()
    return ok, err
end

-- Standard JSON/control opcodes receive callback
function CalibreWireless:JSONReceiveCallback()
    -- NOTE: Closure trickery because we need a reference to *this* self *inside* the callback,
    --       which will be called as a function from another object (namely, StreamMessageQueue).
    local this = self
    return function(t)
        local data = table.concat(t)
        this:onReceiveJSON(data)
    end
end

function CalibreWireless:initCalibreMQ(host, port)
    local StreamMessageQueue = require("ui/message/streammessagequeue")
    if self.calibre_socket == nil then
        self.calibre_socket = StreamMessageQueue:new{
            host = host,
            port = port,
            receiveCallback = self:JSONReceiveCallback(),
        }
        self.calibre_socket:start()
        UIManager:insertZMQ(self.calibre_socket)
    end
end

function CalibreWireless:setInboxDir(cb)
    local force_chooser_dir
    if Device:isAndroid() then
        force_chooser_dir = Device.home_dir
    end

    require("ui/downloadmgr"):new{
        onConfirm = function(inbox)
            local driver = CalibreMetadata:getDeviceInfo(inbox, "device_name")
            local warning = function()
                if not driver then return end
                return not driver:lower():match("koreader") and not driver:lower():match("folder")
            end
            local save_and_cb = function()
                logger.info("set inbox directory", inbox)
                G_reader_settings:saveSetting("inbox_dir", inbox)
                if cb then
                    cb(inbox)
                end
            end
            -- probably not a good idea to mix calibre drivers because
            -- their default settings usually don't match (lpath et al)
            if warning() then
                UIManager:show(ConfirmBox:new{
                    text = T(_([[This folder is already initialized as a %1.

Mixing calibre libraries is not recommended unless you know what you're doing.

Do you want to continue? ]]), driver),

                    ok_text = _("Continue"),
                    ok_callback = save_and_cb
                })
            else
                save_and_cb()
            end
        end,
    }:chooseDir(force_chooser_dir)
end

function CalibreWireless:connect()
	if NetworkMgr:willRerunWhenConnected(function() self:connect() end) then
        return
    end

    if self.calibre_socket ~= nil then
        return
    end

    -- Ensure we're running in a coroutine.
    local co = coroutine.running()
    if not co then
        Trapper:wrap(function() self:connect() end)
        return
    end
    local re = function(res) coroutine.resume(co, res) end

    -- Setup inbox directory.
    local inbox_dir = G_reader_settings:readSetting("inbox_dir")
    if not inbox_dir then
        self:setInboxDir(re)
        inbox_dir = coroutine.yield()
    end

    -- Ensure network is online.
    if NetworkMgr:willRerunWhenConnected(self.re) then
        coroutine.yield()
        if not NetworkMgr:isConnected() then
            return
        end
    end

    local address_type, host, port, ok, err

    -- Setup server address.
    local calibre_url = G_reader_settings:readSetting("calibre_wireless_url")
    if calibre_url then
        host, port = calibre_url["address"], calibre_url["port"]
        address_type = "specified"
        ok = true
    else
        logger.info("calibre: searching for a server")
        Trapper:info(_("Searching for a calibre server… (tap to cancel)"))
        ok, host, port = Trapper:dismissableRunInSubprocess(find_calibre_server)
        if not ok then
            -- Canceled.
            return
        end
        if not host or not port then
            Trapper:info(_("Couldn't discover a calibre instance on the local network"))
            return
        end
        address_type = "discovered"
    end

    local server_info = BD.ltr(T("%1:%2", host, port))

    -- Yield for `timeout` seconds, return:
    -- - `true` if resumed because of some server activity
    -- - `false` on abort / cancelation / disconnection
    -- - `nil` on timeout
    local resume_in = function(timeout)
        UIManager:scheduleIn(timeout, re)
        local result = coroutine.yield()
        UIManager:unschedule(re)
        return result
    end

    if ok then
        -- Start connection.
        logger.info(string.format("calibre: connecting to %s:%s (%s)", host, port, address_type))
        -- @translators %1: address (host:port), %2: address type (discovered, specified, unavailable)
        Trapper:info(T(_("Connecting to calibre server at %1 (%2, tap to cancel)"), server_info, _(address_type)))
        self.re = re
        self.invalid_password = false
        self.disconnected_by_server = false
        if pcall(self.initCalibreMQ, self, host, port) then
            CalibreMetadata:init(inbox_dir)
            -- And wait for initial requests: GET_INITIALIZATION_INFO, followed by
            -- GET_DEVICE_INFORMATION (or a specific DISPLAY_MESSAGE on password error).
            for _, timeout in ipairs{5, 1} do
                ok = resume_in(timeout)
                if not ok then
                    break
                end
            end
        else
            ok = nil
        end
        if ok == false then
            -- Connection was canceled by the user.
            self:disconnect(true)
            return
        end
        if self.invalid_password then
            ok, err = false, "invalid password"
        elseif not ok then
            -- Manually open a TCP connection to get a more informative error.
            ok, err = check_host_port(host, port)
            if ok then
                ok, err = false, "handshake timeout"
            else
                err = err:lower()
            end
        end
    end

    if not ok then
        logger.warn("calibre: connection failed,", err)
        -- @translators %1: address (host:port), %2: error
        Trapper:info(T(_("Cannot connect to calibre server at %1 (%2)"), server_info, _(err)))
        self:disconnect(not self.invalid_password)
        return
    end

    Trapper:clear()

    logger.info("calibre: connected")

    -- Heartbeat monitoring…
    while ok and not self.disconnected_by_server do
        ok = resume_in(5 * 60)
    end

    local msg
    if self.disconnected_by_server then
        logger.info("disconnected by calibre")
        msg = _("Disconnected by calibre")
    elseif ok == nil then
        logger.info("calibre: no activity")
        msg = _("Disconnected from calibre (no activity)")
    end
    if msg then
        UIManager:show(InfoMessage:new{ text = msg, timeout = 2 })
        self:disconnect(not self.disconnected_by_server)
    end

end

function CalibreWireless:disconnect(no_parting_noop)
    if self.calibre_socket == nil then
        return
    end
    logger.info("calibre: disconnecting")

    self.re(false)
    self.re = nil

    if not no_parting_noop then
        self:sendJsonData('NOOP', {})
    end

    UIManager:removeZMQ(self.calibre_socket)
    self.calibre_socket:stop()
    self.calibre_socket = nil
    self.invalid_password = false
    self.disconnected_by_server = false

    CalibreMetadata:clean()

    -- Assume the library content was modified, as such, invalidate our Search metadata cache.
    CalibreSearch:invalidateCache()
end

function CalibreWireless:reconnect()
    -- to use when something went wrong and we aren't in sync with calibre
    FFIUtil.sleep(1)
    self:disconnect()
    FFIUtil.sleep(1)
    self:connect()
end

function CalibreWireless:onReceiveJSON(data)
    self.buffer = (self.buffer or "") .. (data or "")
    --logger.info("data buffer", self.buffer)
    -- messages from calibre stream socket are encoded in JSON strings like this
    -- 34[0, {"key0":value, "key1": value}]
    -- the JSON string has a leading length string field followed by the actual
    -- JSON data in which the first element is always the operator code which can
    -- be looked up in the opcodes dictionary
    while self.buffer ~= nil do
        --logger.info("buffer", self.buffer)
        local index = self.buffer:find('%[') or 1
        local size = tonumber(self.buffer:sub(1, index - 1))
        local json_data
        if size and #self.buffer >= index - 1 + size then
            json_data = self.buffer:sub(index, index - 1 + size)
            --logger.info("json_data", json_data)
            -- reset buffer to nil if all buffer is copied out to json data
            self.buffer = self.buffer:sub(index + size)
            --logger.info("new buffer", self.buffer)
        -- data is not complete which means there are still missing data not received
        else
            return
        end
        local json, err = rapidjson.decode(json_data)
        if json then
            --logger.dbg("received json table", json)
            local opcode = json[1]
            local arg = json[2]
            if opcode == OPCODES.GET_INITIALIZATION_INFO then
                self:getInitInfo(arg)
            elseif opcode == OPCODES.GET_DEVICE_INFORMATION then
                self:getDeviceInfo(arg)
            elseif opcode == OPCODES.SET_CALIBRE_DEVICE_INFO then
                self:setCalibreInfo(arg)
            elseif opcode == OPCODES.FREE_SPACE then
                self:getFreeSpace(arg)
            elseif opcode == OPCODES.SET_LIBRARY_INFO then
                self:setLibraryInfo(arg)
            elseif opcode == OPCODES.GET_BOOK_COUNT then
                self:getBookCount(arg)
			elseif opcode == OPCODES.SEND_BOOKLISTS then
                self:sendBooklists(arg)
            elseif opcode == OPCODES.SEND_BOOK then
                self:sendBook(arg)
            elseif opcode == OPCODES.SEND_BOOK_METADATA then
                self:sendBookMetadata(arg)
            elseif opcode == OPCODES.DELETE_BOOK then
                self:deleteBook(arg)
            elseif opcode == OPCODES.GET_BOOK_FILE_SEGMENT then
                self:sendToCalibre(arg)
            elseif opcode == OPCODES.DISPLAY_MESSAGE then
                self:serverFeedback(arg)
            elseif opcode == OPCODES.NOOP then
                self:noop(arg)
            end
            self.re(true)
        else
            logger.warn("failed to decode json data", err)
        end
    end
end

function CalibreWireless:sendJsonData(opname, data)
    local json, err = rapidjson.encode(rapidjson.array({OPCODES[opname], data}))
    if json then
        -- length of json data should be before the real json data
        self.calibre_socket:send(tostring(#json)..json)
    else
        logger.warn("failed to encode json data", err)
    end
end

function CalibreWireless:getInitInfo(arg)
    logger.dbg("GET_INITIALIZATION_INFO", arg)
    local s = ""
    for i, v in ipairs(arg.calibre_version) do
        if i == #arg.calibre_version then
            s = s .. v
        else
            s = s .. v .. "."
        end
    end
    self.calibre.version = arg.calibre_version
    self.calibre.version_string = s
    local getPasswordHash = function()
        local password = G_reader_settings:readSetting("calibre_wireless_password")
        local challenge = arg.passwordChallenge
        if password and challenge then
            return sha.sha1(password..challenge)
        else
            return ""
        end
    end

    local init_info = {
        appName = self.id,
        acceptedExtensions = extensions,
        cacheUsesLpaths = true,
        canAcceptLibraryInfo = true,
        canDeleteMultipleBooks = true,
        canReceiveBookBinary = true,
        canSendOkToSendbook = true,
        canStreamBooks = true,
        canStreamMetadata = true,
        canUseCachedMetadata = true,
		canAcceptCollections = true,
        isReadSyncCol = G_reader_settings:readSetting("read_name"),
        isReadDateSyncCol = G_reader_settings:readSetting("read_date_name"),
        ccVersionNumber = self.version,
        coverHeight = 240,
        deviceKind = self.model,
        deviceName = T("%1 (%2)", self.id, self.model),
        extensionPathLengths = getExtensionPathLengths(),
        passwordHash = getPasswordHash(),
        maxBookContentPacketLen = 4096,
        useUuidFileNames = false,
        versionOK = true,
    }
    self:sendJsonData('OK', init_info)
end

function CalibreWireless:setPassword()
    local function passwordCheck(p)
        local t = type(p)
        if t == "number" or (t == "string" and p:match("%S")) then
            return true
        end
        return false
    end
    local password_dialog
    password_dialog = InputDialog:new{
        title = _("Set a password for calibre wireless server"),
        input = G_reader_settings:readSetting("calibre_wireless_password") or "",
        buttons = {{
            {
                text = _("Cancel"),
                id = "close",
                callback = function()
                    UIManager:close(password_dialog)
                end,
            },
            {
                text = _("Set password"),
                callback = function()
                    local pass = password_dialog:getInputText()
                    if passwordCheck(pass) then
                        G_reader_settings:saveSetting("calibre_wireless_password", pass)
                    else
                        G_reader_settings:delSetting("calibre_wireless_password")
                    end
                    UIManager:close(password_dialog)
                end,
            },
        }},
    }
    UIManager:show(password_dialog)
    password_dialog:onShowKeyboard()
end

function CalibreWireless:setReadLookupName()
    local function lookupNameCheck(name)
        -- Проверяем, начинается ли строка с символа '#'
        if type(name) == "string" and name:sub(1, 1) == "#" then
            return true
        end
        return false
    end

    local lookup_name_dialog
    lookup_name_dialog = InputDialog:new{
        title = _("Set Read Lookup Name"),
        input = G_reader_settings:readSetting("read_name") or "",
        buttons = {{
            {
                text = _("Cancel"),
                id = "close",
                callback = function()
                    UIManager:close(lookup_name_dialog)
                end,
            },
            {
                text = _("Set Name"),
                callback = function()
                    local name = lookup_name_dialog:getInputText()
                    if lookupNameCheck(name) then
                        G_reader_settings:saveSetting("read_name", name)
                    else
                        G_reader_settings:delSetting("read_name")
                    end
                    UIManager:close(lookup_name_dialog)
                end,
            },
        }},
    }
    UIManager:show(lookup_name_dialog)
    lookup_name_dialog:onShowKeyboard()
end

function CalibreWireless:setReadDateLookupName()
    local function lookupNameCheck(name)
        -- Проверяем, начинается ли строка с символа '#'
        if type(name) == "string" and name:sub(1, 1) == "#" then
            return true
        end
        return false
    end

    local lookup_name_dialog
    lookup_name_dialog = InputDialog:new{
        title = _("Set Read Date Lookup Name"),
        input = G_reader_settings:readSetting("read_date_name") or "",
        buttons = {{
            {
                text = _("Cancel"),
                id = "close",
                callback = function()
                    UIManager:close(lookup_name_dialog)
                end,
            },
            {
                text = _("Set Name"),
                callback = function()
                    local name = lookup_name_dialog:getInputText()
                    if lookupNameCheck(name) then
                        G_reader_settings:saveSetting("read_date_name", name)
                    else
                        G_reader_settings:delSetting("read_date_name")
                    end
                    UIManager:close(lookup_name_dialog)
                end,
            },
        }},
    }
    UIManager:show(lookup_name_dialog)
    lookup_name_dialog:onShowKeyboard()
end

function CalibreWireless:setFavoriteLookupName()
    local function lookupNameCheck(name)
        -- Проверяем, начинается ли строка с символа '#'
        if type(name) == "string" and name:sub(1, 1) == "#" then
            return true
        end
        return false
    end

    local lookup_name_dialog
    lookup_name_dialog = InputDialog:new{
        title = _("Set Favorite Lookup Name"),
        input = G_reader_settings:readSetting("favorite_name") or "",
        buttons = {{
            {
                text = _("Cancel"),
                id = "close",
                callback = function()
                    UIManager:close(lookup_name_dialog)
                end,
            },
            {
                text = _("Set Name"),
                callback = function()
                    local name = lookup_name_dialog:getInputText()
                    if lookupNameCheck(name) then
                        G_reader_settings:saveSetting("favorite_name", name)
                    else
                        G_reader_settings:delSetting("favorite_name")
                    end
                    UIManager:close(lookup_name_dialog)
                end,
            },
        }},
    }
    UIManager:show(lookup_name_dialog)
    lookup_name_dialog:onShowKeyboard()
end

function CalibreWireless:getDeviceInfo(arg)
    logger.dbg("GET_DEVICE_INFORMATION", arg)
    local device_info = {
        device_info = {
           device_store_uuid = CalibreMetadata.drive.device_store_uuid,
           device_name = T("%1 (%2)", self.id, self.model),
        },
        version  = self.version,
        device_version = self.version,
    }
    self:sendJsonData('OK', device_info)
end

function CalibreWireless:setCalibreInfo(arg)
    logger.dbg("SET_CALIBRE_DEVICE_INFO", arg)
    CalibreMetadata:saveDeviceInfo(arg)
    self:sendJsonData('OK', {})
end

function CalibreWireless:getFreeSpace(arg)
    logger.dbg("FREE_SPACE", arg)
    local free_space = {
        free_space_on_device = getFreeSpace(G_reader_settings:readSetting("inbox_dir")),
    }
    self:sendJsonData('OK', free_space)
end

function CalibreWireless:setLibraryInfo(arg)
    logger.dbg("SET_LIBRARY_INFO", arg)
    self:sendJsonData('OK', {})
end

function CalibreWireless:getBookCount(arg)
    logger.dbg("GET_BOOK_COUNT", arg)
    
    local supports_sync = arg.supportsSync
    local read_status_column = G_reader_settings:readSetting("read_name")
    local read_date_column = G_reader_settings:readSetting("read_date_name")
    
    local books = {
        willStream = true,
        willScan = true,
        count = #CalibreMetadata.books,
    }
    self:sendJsonData('OK', books)
    
    -- Send book data based on whether caching is enabled
    for index, book in ipairs(CalibreMetadata.books) do
        local book_data
        
        if arg.willUseCachedMetadata then
            -- When caching is enabled, send minimal data with priKey
            book_data = {
                priKey = index,
                uuid = book.uuid,
                lpath = book.lpath,
                last_modified = book.last_modified,
                extension = book.extension or ""
            }
        else
            -- When caching is disabled, send full metadata
            book_data = CalibreMetadata:getBookMetadata(index)
        end
        
        -- Add sync information if supported
        if supports_sync and (read_status_column or read_date_column) then
            local file_path = G_reader_settings:readSetting("inbox_dir") .. "/" .. book.lpath
            local sdr_dir = DocSettings:getSidecarDir(file_path)
            local metadata_file = sdr_dir .. "/metadata." .. util.getFileNameSuffix(file_path) .. ".lua"
            
            if lfs.attributes(metadata_file, "mode") == "file" then
                local ok, doc_settings = pcall(dofile, metadata_file)
                if ok and doc_settings and doc_settings.summary then
                    local status = doc_settings.summary.status
                    
                    if status == "complete" then
                        if read_status_column then
                            book_data["_is_read_"] = true
                            book_data["_sync_type_"] = "read"
                        end
                        
                        if read_date_column and doc_settings.summary.modified then
                            book_data["_last_read_date_"] = doc_settings.summary.modified
                        end
                    end
                end
            end
        end
        
        self:sendJsonData('OK', book_data)
    end
    
    logger.info("Finished sending book data to Calibre")
end

function CalibreWireless:sendBooklists(arg)
    logger.dbg("SEND_BOOKLISTS", arg)
    
    if arg.collections and (arg.count > 0 or next(arg.collections) ~= nil) then
        local inbox_dir = G_reader_settings:readSetting("inbox_dir")
        
        -- Load ReadCollection
        pcall(ReadCollection.read, ReadCollection)
        
        if not ReadCollection.coll then
            ReadCollection.coll = {}
            ReadCollection.coll_settings = {}
        end
        
        local default_collection_name = ReadCollection.default_collection_name
        local collection_changes = false
        
        -- Track changes for PocketBook sync
        local pb_changes = {
            collections_to_sync = {},
            collections_to_remove = {}
        }
        
        -- Create a set of collections that exist in Calibre for easy lookup
        local calibre_collections = {}
        for collection_name, file_list in pairs(arg.collections) do
            if collection_name and collection_name ~= "" then
                local clean_name = collection_name:match("^(.-)%s*%(.*%)$") or collection_name
                calibre_collections[clean_name] = {}
                for _, lpath in ipairs(file_list) do
                    local file_path = inbox_dir .. "/" .. lpath
                    calibre_collections[clean_name][file_path] = true
                end
            end
        end
        
        -- Step 1: Process existing KOReader collections (except Favorites)
        for collection_name, collection_data in pairs(ReadCollection.coll) do
            if collection_name ~= default_collection_name then -- Skip Favorites collection
                local calibre_files = calibre_collections[collection_name]
                
                if calibre_files then
                    -- Collection exists in both - sync the differences
                    logger.info("Syncing existing collection:", collection_name)
                    
                    -- Track changes for PocketBook sync
                    local pb_collection_data = {
                        files_to_add = {},
                        files_to_remove = {}
                    }
                    
                    -- Remove files that are no longer in Calibre collection
                    local files_to_remove = {}
                    for file_path in pairs(collection_data) do
                        if not calibre_files[file_path] then
                            table.insert(files_to_remove, file_path)
                            pb_collection_data.files_to_remove[file_path] = true
                        end
                    end
                    
                    for _, file_path in ipairs(files_to_remove) do
                        logger.dbg("Removing from collection", collection_name, "file:", file_path)
                        ReadCollection:removeItem(file_path, collection_name, true)
                        collection_changes = true
                    end
                    
                    -- Add files that are new in Calibre collection
                    for file_path in pairs(calibre_files) do
                        if lfs.attributes(file_path, "mode") == "file" and not collection_data[file_path] then
                            logger.dbg("Adding to existing collection", collection_name, "file:", file_path)
                            ReadCollection:addItem(file_path, collection_name)
                            pb_collection_data.files_to_add[file_path] = true
                            collection_changes = true
                        end
                    end
                    
                    -- Add to PocketBook changes if there are any changes
                    if next(pb_collection_data.files_to_add) or next(pb_collection_data.files_to_remove) then
                        pb_changes.collections_to_sync[collection_name] = pb_collection_data
                    end
                    
                    -- Mark this collection as processed
                    calibre_collections[collection_name] = nil
                else
                    -- Collection no longer exists in Calibre - remove it
                    logger.info("Removing collection no longer in Calibre:", collection_name)
                    ReadCollection.coll[collection_name] = nil
                    ReadCollection.coll_settings[collection_name] = nil
                    pb_changes.collections_to_remove[collection_name] = true
                    collection_changes = true
                end
            end
        end
        
        -- Step 2: Create new collections that exist only in Calibre
        for collection_name, file_paths in pairs(calibre_collections) do
            if collection_name and collection_name ~= "" then
                logger.info("Creating new collection from Calibre:", collection_name)
                ReadCollection:addCollection(collection_name)
                
                -- Track new collection for PocketBook sync
                local pb_collection_data = {
                    files_to_add = {},
                    files_to_remove = {}
                }
                
                -- Add files to the new collection
                for file_path in pairs(file_paths) do
                    if lfs.attributes(file_path, "mode") == "file" then
                        ReadCollection:addItem(file_path, collection_name)
                        pb_collection_data.files_to_add[file_path] = true
                        logger.dbg("Added to new collection", collection_name, "file:", file_path)
                    end
                end
                
                pb_changes.collections_to_sync[collection_name] = pb_collection_data
                collection_changes = true
            end
        end
        
        -- Save changes if any were made
        if collection_changes then
            ReadCollection:write()
            logger.info("Collections synchronized with Calibre booklists")
            
            -- Sync PocketBook database if device is PocketBook
            if Device:isPocketBook() then
                PocketBookDBHandler:syncCollectionsIncremental(pb_changes, inbox_dir)
            end
        end
    end
end

function CalibreWireless:noop(arg)
    logger.dbg("NOOP", arg)
    -- calibre wants to close the socket, time to disconnect
    if arg.ejecting then
        self:sendJsonData('OK', {})
        self.disconnected_by_server = true
        return
    end
    -- calibre announces the count of books that need more metadata
    if arg.count then
        self.pending = arg.count
        self.current = 1
        return
    end
    -- calibre requests more metadata for a book by its index
    if arg.priKey then
        local book = CalibreMetadata:getBookMetadata(arg.priKey)
        logger.dbg(string.format("sending book metadata %d/%d", self.current, self.pending))
        self:sendJsonData('OK', book)
        if self.current == self.pending then
            self.current = nil
            self.pending = nil
            return
        end
        self.current = self.current + 1
        return
    end
    -- keep-alive NOOP
    self:sendJsonData('OK', {})
end

function CalibreWireless:sendBook(arg)
    logger.dbg("SEND_BOOK", arg)
    local inbox_dir = G_reader_settings:readSetting("inbox_dir")
    local filename = inbox_dir .. "/" .. arg.lpath
    local fits = getFreeSpace(inbox_dir) >= (arg.length + 128 * 1024)
    local to_write_bytes = arg.length
    local calibre_device = self
    local calibre_socket = self.calibre_socket
    local outfile
    if fits then
        logger.dbg("write to file", filename)
        util.makePath((util.splitFilePathName(filename)))
        outfile = io.open(filename, "wb")
    else
        local msg = T(_("Can't receive file %1/%2: %3\nNo space left on device"),
            arg.thisBook + 1, arg.totalBooks, BD.filepath(filename))
        if self:isCalibreAtLeast(4, 18, 0) then
            -- report the error back to calibre
            self:sendJsonData('ERROR', {message = msg})
            return
        else
            -- report the error in the client
            UIManager:show(InfoMessage:new{
                text = msg,
                timeout = 2,
            })
            self.error_on_copy = true
        end
    end
    -- switching to raw data receiving mode
    self.calibre_socket.receiveCallback = function(t)
        local data = table.concat(t)
        --logger.info("receive file data", #data)
        --logger.info("Memory usage KB:", collectgarbage("count"))
        local to_write_data = data:sub(1, to_write_bytes)
        if fits then
            outfile:write(to_write_data)
        end
        to_write_bytes = to_write_bytes - #to_write_data
        if to_write_bytes == 0 then
            if fits then
                -- close file as all file data is received and written to local storage
                outfile:close()
                logger.dbg("complete writing file", filename)
                -- add book to local database/table
                CalibreMetadata:addBook(arg.metadata)
                UIManager:show(InfoMessage:new{
                    text = T(_("Received file %1/%2: %3"),
                        arg.thisBook + 1, arg.totalBooks, BD.filepath(filename)),
                    timeout = 2,
                })
                CalibreMetadata:saveBookList()
                updateDir(inbox_dir)
				
				if Device:isPocketBook() then
					PocketBookDBHandler:saveBookToDatabase(arg, filename)
				end
				
            end
            -- switch back to JSON data receiving mode
            calibre_socket.receiveCallback = calibre_device:JSONReceiveCallback()
            -- if calibre sends multiple files there may be leftover JSON data
            calibre_device.buffer = data:sub(#to_write_data + 1) or ""
            --logger.info("device buffer", calibre_device.buffer)
            if calibre_device.buffer ~= "" then
                -- since data is already copied to buffer
                -- onReceiveJSON parameter should be nil
                calibre_device:onReceiveJSON()
            end
        end
        self.re(true)
    end
    self:sendJsonData('OK', {})
    -- end of the batch
    if (arg.thisBook + 1) == arg.totalBooks then
        if not self.error_on_copy then return end
        self.error_on_copy = nil
        UIManager:show(ConfirmBox:new{
            text = T(_("Insufficient disk space.\n\ncalibre %1 will report all books as in device. This might lead to errors. Please reconnect to get updated info"),
                self.calibre.version_string),
            ok_text = _("Reconnect"),
            ok_callback = function()
                -- send some info to avoid harmless but annoying exceptions in calibre
                self:getFreeSpace()
                self:getBookCount()
                -- scheduled because it blocks!
                UIManager:scheduleIn(1, function()
                    self:reconnect()
                end)
            end,
        })
    end
end

function CalibreWireless:sendBookMetadata(arg)
    logger.info("SEND_BOOK_METADATA", arg)
    
    -- Get book metadata from calibre
    local book_data = arg.data
    if not book_data or not book_data.lpath then
        logger.warn("Invalid book data received")
        return
    end
    
    -- Get sync column settings
    local read_status_column = G_reader_settings:readSetting("read_name")
    local read_date_column = G_reader_settings:readSetting("read_date_name")
    local favorite_column = G_reader_settings:readSetting("favorite_name")
    
    -- Check if sync is supported
    local supports_sync = arg.supportsSync
    if not supports_sync or (not read_status_column and not read_date_column and not favorite_column) then
        logger.info("Sync not supported or columns not configured")
        return
    end
    
    -- Get file path
    local inbox_dir = G_reader_settings:readSetting("inbox_dir")
    local file_path = inbox_dir .. "/" .. book_data.lpath
    
    -- Check if file exists
    if lfs.attributes(file_path, "mode") ~= "file" then
        logger.warn("Book file not found:", file_path)
        return
    end
    
    -- Open document settings
    local doc_settings = DocSettings:open(file_path)
    if not doc_settings then
        logger.warn("Failed to open doc settings for:", file_path)
        return
    end
    
    local settings_changed = false
    
    -- Update read status if the column exists and has a value
    if read_status_column and book_data.user_metadata and book_data.user_metadata[read_status_column] then
        local is_read = book_data.user_metadata[read_status_column]["#value#"]
        local summary = doc_settings:readSetting("summary", {})
        
        if is_read then
            -- Mark as complete if calibre says it's read
            if summary.status ~= "complete" then
                logger.info("Marking book as read:", book_data.lpath)
                summary.status = "complete"
                settings_changed = true
            end
        else
            -- If calibre says not read, but book was complete, mark as reading
            if summary.status == "complete" then
                logger.info("Unmarking book as read:", book_data.lpath)
                summary.status = "reading"
                settings_changed = true
            end
        end
        
        if settings_changed then
            doc_settings:saveSetting("summary", summary)
        end
    end
    
    -- Update read date if the column exists and has a value
    if read_date_column and book_data.user_metadata and book_data.user_metadata[read_date_column] then
        local read_date = book_data.user_metadata[read_date_column]["#value#"]
        local summary = doc_settings:readSetting("summary", {})
        
        -- Only update if the date is different
        if summary.modified ~= read_date then
            logger.info("Updating read date for:", book_data.lpath, "to:", read_date)
            summary.modified = read_date
            doc_settings:saveSetting("summary", summary)
            settings_changed = true
        end
    end
    
    -- Handle ONLY favorite collection synchronization
    if favorite_column and book_data.user_metadata and book_data.user_metadata[favorite_column] then
        pcall(ReadCollection.read, ReadCollection)
        
        if not ReadCollection.coll then
            ReadCollection.coll = {}
            ReadCollection.coll_settings = {}
        end
        
        local collection_changes = false
        local default_collection_name = ReadCollection.default_collection_name
        local calibre_favorite_data = book_data.user_metadata[favorite_column]["#value#"]
        
        if not ReadCollection.coll[default_collection_name] then
            ReadCollection:addCollection(default_collection_name)
        end
        
        local is_in_koreader_favorites = ReadCollection:isFileInCollection(file_path, default_collection_name)
        local is_calibre_favorite = (calibre_favorite_data == true)
        
        if is_calibre_favorite and not is_in_koreader_favorites then
            logger.info("Syncing: Adding book to KOReader favorites from Calibre:", book_data.lpath)
            ReadCollection:addItem(file_path, default_collection_name)
            collection_changes = true
        elseif not is_calibre_favorite and is_in_koreader_favorites then
            logger.info("Syncing: Removing book from KOReader favorites per Calibre:", book_data.lpath)
            ReadCollection:removeItem(file_path, default_collection_name, true)
            collection_changes = true
        end
        
        -- Save collection changes if any were made
        if collection_changes then
            ReadCollection:write()
            logger.info("Updated favorites for:", book_data.lpath)
        end
    end
    
    -- Save document settings changes if any were made
    if settings_changed then
        doc_settings:flush()
        logger.info("Updated reading metadata for:", book_data.lpath)
    end
    
    -- Update PocketBook database if running on PocketBook device
    if Device:isPocketBook() then
        PocketBookDBHandler:updateBookMetadata(book_data, file_path)
    end
end

function CalibreWireless:deleteBook(arg)
    logger.dbg("DELETE_BOOK", arg)
    self:sendJsonData('OK', {})
    local inbox_dir = G_reader_settings:readSetting("inbox_dir")
    if not inbox_dir then return end
    -- remove all books requested by calibre
    local titles = ""
    for i, v in ipairs(arg.lpaths) do
        local book_uuid, index = CalibreMetadata:getBookUuid(v)
        if not index then
            logger.warn("requested to delete a book no longer on device", arg.lpaths[i])
        else
            titles = titles .. "\n" .. CalibreMetadata.books[index].title
            util.removeFile(inbox_dir.."/"..v)
            CalibreMetadata:removeBook(v)
        end
        self:sendJsonData('OK', { uuid = book_uuid })
        -- do things once at the end of the batch
        if i == #arg.lpaths then
            local msg
            if i == 1 then
                msg = T(_("Deleted file: %1"), BD.filepath(arg.lpaths[1]))
            else
                msg = T(_("Deleted %1 files in %2:\n %3"),
                    #arg.lpaths, BD.filepath(inbox_dir), titles)
            end
            UIManager:show(InfoMessage:new{
                text = msg,
                timeout = 2,
            })
            CalibreMetadata:saveBookList()
            updateDir(inbox_dir)
        end
    end
end

function CalibreWireless:serverFeedback(arg)
    logger.dbg("DISPLAY_MESSAGE", arg)
    -- here we only care about password errors
    if arg.messageKind == 1 then
        self.invalid_password = true
    end
end

function CalibreWireless:sendToCalibre(arg)
    logger.dbg("GET_BOOK_FILE_SEGMENT", arg)
    local inbox_dir = G_reader_settings:readSetting("inbox_dir")
    local path = inbox_dir .. "/" .. arg.lpath

    local file_size = lfs.attributes(path, "size")
    if not file_size then
        self:sendJsonData("NOOP", {})
        return
    end

    local file = io.open(path, "rb")
    if not file then
        self:sendJsonData("NOOP", {})
        return
    end

    self:sendJsonData("OK", { fileLength = file_size })

    while true do
        local data = file:read(4096)
        if not data then break end
        self.calibre_socket:send(data)
    end

    file:close()
end

function CalibreWireless:isCalibreAtLeast(x, y, z)
    local v = self.calibre.version
    local function semanticVersion(a, b, c)
        return ((a * 100000) + (b * 1000)) + c
    end
    return semanticVersion(v[1], v[2], v[3]) >= semanticVersion(x, y, z)
end

return CalibreWireless
