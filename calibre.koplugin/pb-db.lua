--[[
	This module adds books transferred using the "smart device app" protocol of the calibre to the Pocketbook database
--]]

local logger = require("logger")
local lfs = require("libs/libkoreader-lfs")
local SQ3 = require("lua-ljsqlite3/init")
local ffi = require("ffi")
local inkview = ffi.load("inkview")
local current_timestamp = os.time()
local db_path = "/mnt/ext1/system/explorer-3/explorer-3.db"

local PocketBookDBHandler = {}

local function get_storage_id(filename)
    if string.match(filename, "^/mnt/ext1") then
        return 1
    else
        return 2
    end
end

function PocketBookDBHandler:saveBookToDatabase(arg, filename)
    collections_lookup_name = G_reader_settings:readSetting("collections_name")
    read_lookup_name = G_reader_settings:readSetting("read_name")
    read_date_lookup_name = G_reader_settings:readSetting("read_date_name")
    favorite_lookup_name = G_reader_settings:readSetting("favorite_name")
    
    local function getFirstLetter(str)
        if not str or str == "" then return "" end
        local first = str:sub(1,1):upper()
        return first:match("[%w%p]") and first or str:sub(1,2):upper()
    end

    local function safeToString(value)
        if type(value) == "userdata" then return "" end
        return tostring(value or "")
    end

    local function formatAuthorName(author)
        if not author or author == "" then return "" end
        local parts = {}
        for part in author:gmatch("%S+") do
            table.insert(parts, part)
        end
        if #parts >= 2 then
            local lastName = parts[#parts]
            table.remove(parts)
            local firstName = table.concat(parts, " ")
            return lastName .. ", " .. firstName
        end
        return author
    end

    local db = SQ3.open(db_path, "rw")
    if not db then
        logger.info("Error: Failed to open database")
        return
    end
    db:exec("PRAGMA busy_timeout = 5000;")

    local closed = false
    local function safe_db_close()
        if not closed then
            db:close()
            closed = true
        end
    end

    db:exec("BEGIN TRANSACTION")
    
    local success = true
    local folder_id, book_id
    local folder = filename:match("(.+)/[^/]+$")
    local storage_id = get_storage_id(filename)
    local file_name = filename:match("/([^/]+)$")
    local file_ext = file_name:match("%.([^%.]+)$")

    if not arg.metadata or not arg.metadata.title or arg.metadata.title == "" then
        logger.info("Error: Missing metadata or book title")
        db:exec("ROLLBACK")
        safe_db_close()
        return
    end

    local file_stats = lfs.attributes(filename)
    if not file_stats then
        logger.info("Error: Failed to get file attributes")
        db:exec("ROLLBACK")
        safe_db_close()
        return
    end

    if not folder or folder == "" then
        logger.info("Error: Empty folder path: " .. filename)
        db:exec("ROLLBACK")
        safe_db_close()
        return
    end

    -- Process folder
    local insert_folder_sql = [[
        INSERT INTO folders (storageid, name) 
        VALUES (?, ?) 
        ON CONFLICT(storageid, name) DO NOTHING;
    ]]

    local select_folder_sql = [[
        SELECT id FROM folders 
        WHERE storageid = ? AND name = ?;
    ]]

    local folder_stmt = db:prepare(insert_folder_sql)
    if not folder_stmt then
        logger.info("Error: Failed to prepare folder SQL query")
        db:exec("ROLLBACK")
        safe_db_close()
        return
    end

    folder_stmt:bind(storage_id, folder)
    folder_stmt:step()
    folder_stmt:close()

    local select_stmt = db:prepare(select_folder_sql)
    if not select_stmt then
        logger.info("Error: Failed to prepare SELECT query")
        db:exec("ROLLBACK")
        safe_db_close()
        return
    end

    select_stmt:bind(storage_id, folder)
    local row = select_stmt:step()
    select_stmt:close()

    if type(row) == "table" then
        folder_id = row[1]
    end

    if not folder_id then
        logger.info("Error: Failed to get folder ID")
        db:exec("ROLLBACK")
        safe_db_close()
        return
    end

    local author = table.concat(arg.metadata.authors or {}, ", ")
    local title = safeToString(arg.metadata.title)
    
    -- Check if file already exists
    local check_file_sql = [[
        SELECT id, book_id FROM files 
        WHERE filename = ? AND folder_id = ?;
    ]]

    local check_file_stmt = db:prepare(check_file_sql)
    if not check_file_stmt then
        logger.info("Error: Failed to prepare file check query")
        db:exec("ROLLBACK")
        safe_db_close()
        return
    end
    
    check_file_stmt:bind1(1, file_name)
    check_file_stmt:bind1(2, folder_id)
    local existing_file = check_file_stmt:step()
    check_file_stmt:close()

    if type(existing_file) == "table" then
        -- File exists, update it
        local file_id = existing_file[1]
        book_id = existing_file[2]
        
        -- Update file info
        local update_file_sql = [[
            UPDATE files SET
                size = ?,
                modification_time = ?
            WHERE id = ?;
        ]]
        
        local update_file_stmt = db:prepare(update_file_sql)
        if not update_file_stmt then
            logger.info("Error: Failed to prepare file update query")
            db:exec("ROLLBACK")
            safe_db_close()
            return
        end
        
        update_file_stmt:bind1(1, file_stats.size)
        update_file_stmt:bind1(2, file_stats.modification)
        update_file_stmt:bind1(3, file_id)

        if update_file_stmt:step() ~= SQ3.DONE then
            logger.info("Error updating file")
            success = false
        end
        update_file_stmt:close()
        
        -- Update book info
        local update_book_sql = [[
            UPDATE books_impl SET
                title = ?,
                first_title_letter = ?,
                author = ?,
                firstauthor = ?,
                first_author_letter = ?,
                series = ?,
                numinseries = ?,
                size = ?,
                isbn = ?,
                sort_title = ?,
                updated = ?,
                ts_added = ?
            WHERE id = ?;
        ]]
        
        local update_book_stmt = db:prepare(update_book_sql)
        if not update_book_stmt then
            logger.info("Error: Failed to prepare book update query")
            db:exec("ROLLBACK")
            safe_db_close()
            return
        end
        
        update_book_stmt:bind1(1, title)
        update_book_stmt:bind1(2, getFirstLetter(title))
        update_book_stmt:bind1(3, author)
        update_book_stmt:bind1(4, arg.metadata.author_sort)
        update_book_stmt:bind1(5, getFirstLetter(arg.metadata.author_sort))
        update_book_stmt:bind1(6, safeToString(arg.metadata.series))
        update_book_stmt:bind1(7, tonumber(safeToString(arg.metadata.series_index)) or 0)
        update_book_stmt:bind1(8, arg.metadata.size)
        update_book_stmt:bind1(9, safeToString(arg.metadata.isbn))
        update_book_stmt:bind1(10, title)
        update_book_stmt:bind1(11, current_timestamp)
        update_book_stmt:bind1(12, current_timestamp)
        update_book_stmt:bind1(13, book_id)

        if update_book_stmt:step() ~= SQ3.DONE then
            logger.info("Error updating book")
            success = false
        end
        update_book_stmt:close()
        
    else
        -- File doesn't exist, create new book and file records
        logger.info("Creating new book and file records")
        
        -- Add new book
        local book_sql = [[
            INSERT INTO books_impl (
                title, first_title_letter, author, firstauthor, 
                first_author_letter, series, numinseries, size, 
                isbn, sort_title, creationtime, updated, 
                ts_added, hidden
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ]]
        
        local book_stmt = db:prepare(book_sql)
        if not book_stmt then
            logger.info("Error: Failed to prepare book SQL query")
            db:exec("ROLLBACK")
            safe_db_close()
            return
        end

        book_stmt:bind1(1, title)
        book_stmt:bind1(2, getFirstLetter(title))
        book_stmt:bind1(3, author)
        book_stmt:bind1(4, arg.metadata.author_sort)
        book_stmt:bind1(5, getFirstLetter(arg.metadata.author_sort))
        book_stmt:bind1(6, safeToString(arg.metadata.series))
        book_stmt:bind1(7, tonumber(safeToString(arg.metadata.series_index)) or 0)
        book_stmt:bind1(8, arg.metadata.size)
        book_stmt:bind1(9, safeToString(arg.metadata.isbn))
        book_stmt:bind1(10, title)
        book_stmt:bind1(11, 0)
        book_stmt:bind1(12, 0)
        book_stmt:bind1(13, current_timestamp)
        book_stmt:bind1(14, 0)

        if book_stmt:step() ~= SQ3.DONE then
            logger.info("Error adding book")
            db:exec("ROLLBACK")
            safe_db_close()
            return
        end

        book_id = db:rowexec("SELECT last_insert_rowid()")
        book_stmt:close()

        if not book_id then
            logger.info("Error: Failed to get book ID")
            db:exec("ROLLBACK")
            safe_db_close()
            return
        end
        
        -- Add new file
        local files_sql = [[
            INSERT INTO files (
                storageid, folder_id, book_id, filename, 
                size, modification_time, ext
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ]]
        
        local files_stmt = db:prepare(files_sql)
        if not files_stmt then
            logger.info("Error: Failed to prepare file SQL query")
            db:exec("ROLLBACK")
            safe_db_close()
            return
        end

        files_stmt:bind1(1, storage_id)
        files_stmt:bind1(2, folder_id)
        files_stmt:bind1(3, book_id)
        files_stmt:bind1(4, file_name)
        files_stmt:bind1(5, file_stats.size)
        files_stmt:bind1(6, file_stats.modification)
        files_stmt:bind1(7, file_ext)

        if files_stmt:step() ~= SQ3.DONE then
            logger.info("Error adding file")
            success = false
        end
        files_stmt:close()
    end

    if success then
        -- Check for tags in metadata
        local function GetCurrentProfileId()
            local profile_name = inkview.GetCurrentProfile()
            if profile_name == nil then
                return 1
            else
                local stmt = db:prepare("SELECT id FROM profiles WHERE name = ?")
                local profile_id = stmt:reset():bind(ffi.string(profile_name)):step()
                stmt:close()
                return profile_id[1]
            end
        end
        
        local profile_id = GetCurrentProfileId()
        
        local has_read = arg.metadata.user_metadata 
            and arg.metadata.user_metadata[read_lookup_name] 
            and arg.metadata.user_metadata[read_lookup_name]["#value#"] == true

        local has_favorite = arg.metadata.user_metadata 
            and arg.metadata.user_metadata[favorite_lookup_name] 
            and arg.metadata.user_metadata[favorite_lookup_name]["#value#"] == true

        -- Get read date if available
        local completed_date = nil
        if has_read and arg.metadata.user_metadata and arg.metadata.user_metadata[read_date_lookup_name] then
            local date_value = arg.metadata.user_metadata[read_date_lookup_name]["#value#"]
            
            if date_value and date_value ~= "" then
                -- Convert calibre datetime string to YYYY-MM-DD format
                local year, month, day, hour, min, sec = date_value:match("(%d+)-(%d+)-(%d+)T(%d+):(%d+):(%d+)")
                
                if year and month and day and hour and min and sec then
                    -- Convert UTC to local time
                    local utc_time = os.time({
                        year = tonumber(year),
                        month = tonumber(month),
                        day = tonumber(day),
                        hour = tonumber(hour),
                        min = tonumber(min),
                        sec = tonumber(sec)
                    })
                    
                    -- Get local time
                    local local_time = os.date("*t", utc_time)
                    
                    completed_date = string.format("%04d-%02d-%02d", 
                        local_time.year, local_time.month, local_time.day)
                end
            end
        end
        
        -- Save to DocSettings
        if has_read or has_favorite or completed_date then
            local DocSettings = require("docsettings")
            local doc_settings = DocSettings:open(filename)
            
            if has_read then
                doc_settings:saveSetting("summary", {status = "complete"})
            end
            
            if completed_date then
                doc_settings:saveSetting("summary", {status = "complete", modified = completed_date})
            end
            
            doc_settings:flush()
        end
        
        -- Process read/favorite status in database
        if has_read or has_favorite then
            local completed = has_read and 1 or 0
            local favorite = has_favorite and 1 or 0
            
            local select_settings_sql = [[
                SELECT bookid FROM books_settings 
                WHERE bookid = ? AND profileid = ?;
            ]]
            
            local select_settings_stmt = db:prepare(select_settings_sql)
            if not select_settings_stmt then
                logger.info("Error: Failed to prepare settings check query")
                success = false
            else
                select_settings_stmt:bind1(1, book_id)
                select_settings_stmt:bind1(2, profile_id)
                local settings_row = select_settings_stmt:step()
                select_settings_stmt:close()

                if type(settings_row) == "table" then
                    -- Update existing settings
                    local update_settings_sql = [[
                        UPDATE books_settings 
                        SET completed = ?, favorite = ?
                        WHERE bookid = ? AND profileid = ?;
                    ]]
                    
                    local update_settings_stmt = db:prepare(update_settings_sql)
                    if not update_settings_stmt then
                        logger.info("Error: Failed to prepare settings update query")
                        success = false
                    else
                        update_settings_stmt:bind1(1, completed)
                        update_settings_stmt:bind1(2, favorite)
                        update_settings_stmt:bind1(3, book_id)
                        update_settings_stmt:bind1(4, profile_id)
                        
                        if update_settings_stmt:step() ~= SQ3.DONE then
                            logger.info("Error updating settings")
                            success = false
                        end
                        update_settings_stmt:close()
                    end
                else
                    -- Create new settings record
                    local insert_settings_sql = [[
                        INSERT INTO books_settings (bookid, profileid, completed, favorite)
                        VALUES (?, ?, ?, ?);
                    ]]
                    
                    local insert_settings_stmt = db:prepare(insert_settings_sql)
                    if not insert_settings_stmt then
                        logger.info("Error: Failed to prepare settings insert query")
                        success = false
                    else
                        insert_settings_stmt:bind1(1, book_id)
                        insert_settings_stmt:bind1(2, profile_id)
                        insert_settings_stmt:bind1(3, completed)
                        insert_settings_stmt:bind1(4, favorite)
                        
                        if insert_settings_stmt:step() ~= SQ3.DONE then
                            logger.info("Error creating settings")
                            success = false
                        end
                        insert_settings_stmt:close()
                    end
                end
            end
        end
    end

    if success then
        db:exec("COMMIT")
        db:exec("PRAGMA wal_checkpoint(FULL)")
        logger.info("Транзакция успешно завершена, выполнен checkpoint")
    else
        db:exec("ROLLBACK")
        logger.info("Выполнен откат транзакции")
    end

    safe_db_close()
    logger.info("База данных закрыта")
end

-- Function to process collections from SEND_BOOKLISTS
function PocketBookDBHandler:processCollections(collections_data)
    
    if not collections_data.collections then
        logger.info("Collections data missing")
        return
    end
    
    logger.info("=== Starting collections processing from SEND_BOOKLISTS ===")
    
    local db = SQ3.open(db_path, "rw")
    if not db then
        logger.info("Error: Failed to open database for collections processing")
        return
    end

    -- Local function for safe database closing
    local closed = false
    local function safe_db_close()
        if not closed then
            db:close()
            closed = true
        end
    end
    
    db:exec("PRAGMA busy_timeout = 5000;")
    db:exec("BEGIN TRANSACTION")
    
    local success = true
    local current_timestamp = os.time()
    
    -- Store processed collection IDs for subsequent updates
    local processed_bookshelf_ids = {}
    
    -- Process each collection
    for collection_full_name, book_paths in pairs(collections_data.collections) do
        -- Extract collection name from full name
        local collection_name =
            (collection_full_name:match('%["([^"]+)"%]')
            or collection_full_name:match('«(.-)»')
            or collection_full_name)
            :gsub('%s*%b()%s*$', '')

        logger.info("Processing collection: " .. collection_name .. " (full name: " .. collection_full_name .. ")")
        
        -- Find or create collection
        local bookshelf_id = self:findOrCreateBookshelf(db, collection_name, current_timestamp)
        
        if bookshelf_id then
            -- Add ID to processed collections list
            table.insert(processed_bookshelf_ids, bookshelf_id)
            
            -- Process each book in collection
            for _, book_path in ipairs(book_paths) do
                local book_id = self:findBookIdByPath(db, book_path)
                
                if book_id then
                    -- Link book to collection if not already linked
                    self:linkBookToBookshelf(db, bookshelf_id, book_id, current_timestamp)
                else
                    logger.info("Book not found by path: " .. book_path)
                end
            end
        else
            logger.info("Failed to create/find collection: " .. collection_name)
            success = false
            break
        end
    end
    
    -- Force system cache update
    if success then
        logger.info("=== Starting forced system cache update ===")
        
        -- 1. Force update related books to trigger triggers
        for _, bookshelf_id in ipairs(processed_bookshelf_ids) do
            local update_books_sql = [[
                UPDATE books_impl SET updated = ? 
                WHERE id IN (
                    SELECT DISTINCT bb.bookid 
                    FROM bookshelfs_books bb 
                    WHERE bb.bookshelfid = ? AND bb.is_deleted = 0
                )
            ]]
            
            local update_stmt = db:prepare(update_books_sql)
            if update_stmt then
                update_stmt:bind1(1, current_timestamp)
                update_stmt:bind1(2, bookshelf_id)
                
                if update_stmt:step() ~= SQ3.DONE then
                    logger.info("Warning: Failed to update books in collection " .. bookshelf_id)
                end
                update_stmt:close()
            end
        end
        
        -- 2. Update collection timestamps
        for _, bookshelf_id in ipairs(processed_bookshelf_ids) do
            local update_bookshelf_sql = "UPDATE bookshelfs SET ts = ? WHERE id = ?"
            local update_stmt = db:prepare(update_bookshelf_sql)
            if update_stmt then
                update_stmt:bind1(1, current_timestamp)
                update_stmt:bind1(2, bookshelf_id)
                update_stmt:step()
                update_stmt:close()
            end
        end
        
        -- 3. Update timestamps of all book-collection links
        for _, bookshelf_id in ipairs(processed_bookshelf_ids) do
            local update_links_sql = "UPDATE bookshelfs_books SET ts = ? WHERE bookshelfid = ? AND is_deleted = 0"
            local update_stmt = db:prepare(update_links_sql)
            if update_stmt then
                update_stmt:bind1(1, current_timestamp)
                update_stmt:bind1(2, bookshelf_id)
                update_stmt:step()
                update_stmt:close()
            end
        end
        
        logger.info("System cache forcibly updated")
    end
	
    if success then
        db:exec("COMMIT")
        db:exec("PRAGMA wal_checkpoint(FULL)")
        logger.info("Collections successfully processed")
    else
        db:exec("ROLLBACK")
        logger.info("Rollback executed during collections processing")
    end

    safe_db_close()
    logger.info("=== End of collections processing from SEND_BOOKLISTS ===")
end

-- Helper function to find or create bookshelf
function PocketBookDBHandler:findOrCreateBookshelf(db, collection_name, current_timestamp)
    -- Look for existing collection
    local select_sql = "SELECT id FROM bookshelfs WHERE name = ?"
    local select_stmt = db:prepare(select_sql)
    
    if not select_stmt then
        logger.info("Error preparing collection search query")
        return nil
    end
    
    select_stmt:bind1(1, collection_name)
    local row = select_stmt:step()
    select_stmt:close()
    
    if type(row) == "table" then
        local bookshelf_id = row[1]
        
        -- Update is_deleted = 0 and timestamp for found collection
        local update_sql = "UPDATE bookshelfs SET is_deleted = 0, ts = ? WHERE id = ?"
        local update_stmt = db:prepare(update_sql)
        
        if update_stmt then
            update_stmt:bind1(1, current_timestamp)
            update_stmt:bind1(2, bookshelf_id)
            update_stmt:step()
            update_stmt:close()
        end
        
        return bookshelf_id
    else
        -- Create new collection
        local insert_sql = "INSERT INTO bookshelfs (name, is_deleted, ts, uuid) VALUES (?, 0, ?, NULL)"
        local insert_stmt = db:prepare(insert_sql)
        
        if not insert_stmt then
            logger.info("Error preparing collection creation query")
            return nil
        end
        
        insert_stmt:bind1(1, collection_name)
        insert_stmt:bind1(2, current_timestamp)
        
        if insert_stmt:step() == SQ3.DONE then
            local bookshelf_id = db:rowexec("SELECT last_insert_rowid()")
            insert_stmt:close()
            return bookshelf_id
        else
            insert_stmt:close()
            logger.info("Error creating collection")
            return nil
        end
    end
end

-- Helper function to find book ID by path
function PocketBookDBHandler:findBookIdByPath(db, book_path)
    -- Extract filename from path
    local filename = book_path:match("/([^/]+)$")
    if not filename then
        logger.info("Failed to extract filename from path: " .. book_path)
        return nil
    end
    
    -- Search for book by filename
    local sql = [[
        SELECT f.book_id 
        FROM files f 
        WHERE f.filename = ?
        LIMIT 1
    ]]
    
    local stmt = db:prepare(sql)
    if not stmt then
        logger.info("Error preparing book search query")
        return nil
    end
    
    stmt:bind1(1, filename)
    local row = stmt:step()
    stmt:close()
    
    if type(row) == "table" then
        local book_id = row[1]
        return book_id
    else
        return nil
    end
end

-- Helper function to link book to bookshelf
function PocketBookDBHandler:linkBookToBookshelf(db, bookshelf_id, book_id, current_timestamp)
    -- Check for existing link
    local check_sql = "SELECT 1 FROM bookshelfs_books WHERE bookshelfid = ? AND bookid = ?"
    local check_stmt = db:prepare(check_sql)
    
    if not check_stmt then
        logger.info("Error preparing link check query")
        return false
    end
    
    check_stmt:bind1(1, bookshelf_id)
    check_stmt:bind1(2, book_id)
    local existing = check_stmt:step()
    check_stmt:close()
    
    if type(existing) == "table" then
        -- Link already exists, update is_deleted = 0 and timestamp
        local update_sql = "UPDATE bookshelfs_books SET is_deleted = 0, ts = ? WHERE bookshelfid = ? AND bookid = ?"
        local update_stmt = db:prepare(update_sql)
        
        if update_stmt then
            update_stmt:bind1(1, current_timestamp)
            update_stmt:bind1(2, bookshelf_id)
            update_stmt:bind1(3, book_id)
            update_stmt:step()
            update_stmt:close()
        end
        return true
    else
        -- Create new link
        local insert_sql = "INSERT INTO bookshelfs_books (bookshelfid, bookid, ts, is_deleted) VALUES (?, ?, ?, 0)"
        local insert_stmt = db:prepare(insert_sql)
        
        if not insert_stmt then
            logger.info("Error preparing link creation query")
            return false
        end
        
        insert_stmt:bind1(1, bookshelf_id)
        insert_stmt:bind1(2, book_id)
        insert_stmt:bind1(3, current_timestamp)
        
        if insert_stmt:step() == SQ3.DONE then
            insert_stmt:close()
            return true
        else
            insert_stmt:close()
            logger.info("Error creating book-collection link")
            return false
        end
    end
end

return PocketBookDBHandler