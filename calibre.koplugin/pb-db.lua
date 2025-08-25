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
		
		-- Process read/favorite status in database
		if has_read or has_favorite then
			local completed = has_read and 1 or 0
			local favorite = has_favorite and 1 or 0
			local cpage_value = has_read and 100 or nil  -- 100 только если прочитана, иначе NULL
			local npage_value = has_read and 100 or nil  -- 100 только если прочитана, иначе NULL
			
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
			local cpage_value = is_read and 100 or nil
			local npage_value = is_read and 100 or nil
					local update_settings_sql = [[
						UPDATE books_settings 
						SET completed = ?, favorite = ?, cpage = ?, npage = ?
						WHERE bookid = ? AND profileid = ?;
					]]
					
					local update_settings_stmt = db:prepare(update_settings_sql)
					if not update_settings_stmt then
						logger.info("Error: Failed to prepare settings update query")
						success = false
					else
						update_settings_stmt:bind1(1, completed)
						update_settings_stmt:bind1(2, favorite)
						update_settings_stmt:bind1(3, cpage_value)
						update_settings_stmt:bind1(4, npage_value)
						update_settings_stmt:bind1(5, book_id)
						update_settings_stmt:bind1(6, profile_id)
						
						if update_settings_stmt:step() ~= SQ3.DONE then
							logger.info("Error updating settings")
							success = false
						end
						update_settings_stmt:close()
					end
				else
					-- Create new settings record
			local cpage_value = is_read and 100 or nil
			local npage_value = is_read and 100 or nil
					local insert_settings_sql = [[
						INSERT INTO books_settings (bookid, profileid, completed, favorite, cpage, npage)
						VALUES (?, ?, ?, ?, ?, ?);
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
						insert_settings_stmt:bind1(5, cpage_value)
						insert_settings_stmt:bind1(6, npage_value)
						
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

function PocketBookDBHandler:syncCollectionsIncremental(collection_changes, inbox_dir)
    local db = SQ3.open(db_path, "rw")
    if not db then
        logger.warn("Не удалось открыть базу данных PocketBook")
        return false
    end
    
    db:exec("PRAGMA busy_timeout = 5000;")
    db:exec("BEGIN TRANSACTION")
    
    local current_timestamp = os.time()
    local success = true
    
    logger.info("Начало инкрементальной синхронизации коллекций PocketBook")
    
    -- Process collections to be created or updated
    for collection_name, files_data in pairs(collection_changes.collections_to_sync or {}) do
        logger.dbg("Синхронизируем коллекцию:", collection_name)
        
        local bookshelf_id = self:getOrCreateBookshelf(db, collection_name, current_timestamp)
        if not bookshelf_id then
            logger.warn("Failed to get/create bookshelf:", collection_name)
            success = false
            break
        end
        
        -- Add new files
        local files_added = 0
        local files_failed = 0
        for file_path in pairs(files_data.files_to_add or {}) do
            local book_id = self:findBookByPath(db, file_path, inbox_dir)
            if book_id then
                if self:createBookshelfLink(db, bookshelf_id, book_id, current_timestamp) then
                    files_added = files_added + 1
                    logger.dbg("Добавлена книга в коллекцию:", file_path, "->", collection_name)
                else
                    files_failed = files_failed + 1
                    logger.warn("Failed to add book to collection:", file_path, "->", collection_name)
                end
            else
                files_failed = files_failed + 1
                logger.warn("Book not found for collection sync:", file_path)
            end
        end
        
        -- Remove files
        local files_removed = 0
        for file_path in pairs(files_data.files_to_remove or {}) do
            local book_id = self:findBookByPath(db, file_path, inbox_dir)
            if book_id then
                self:removeBookshelfLink(db, bookshelf_id, book_id, current_timestamp)
                files_removed = files_removed + 1
                logger.dbg("Удалена книга из коллекции:", file_path, "->", collection_name)
            end
        end
        
        logger.info(string.format("Коллекция %s: добавлено %d, удалено %d, ошибок %d", 
                   collection_name, files_added, files_removed, files_failed))
        
        -- Если есть ошибки, но не критические, продолжаем
        if files_failed > 0 and files_added == 0 and files_removed == 0 then
            logger.warn("No successful operations for collection:", collection_name)
        end
    end
    
    -- Process collections to be removed
    for collection_name in pairs(collection_changes.collections_to_remove or {}) do
        logger.dbg("Удаляем коллекцию:", collection_name)
        self:removeBookshelf(db, collection_name, current_timestamp)
    end
    
    if success then
        db:exec("COMMIT")
        db:exec("PRAGMA wal_checkpoint(FULL)")
        logger.info("Инкрементальная синхронизация коллекций PocketBook завершена успешно")
    else
        db:exec("ROLLBACK")
        logger.warn("Ошибка синхронизации коллекций, выполнен откат")
    end
    
    db:close()
    return success
end

function PocketBookDBHandler:getOrCreateBookshelf(db, collection_name, timestamp)
    -- Check if bookshelf exists
    local select_sql = "SELECT id FROM bookshelfs WHERE name = ?"
    local stmt = db:prepare(select_sql)
    if not stmt then return nil end
    
    stmt:bind1(1, collection_name)
    local row = stmt:step()
    stmt:close()
    
    if type(row) == "table" then
        local bookshelf_id = row[1]
        -- Reactivate if deleted
        local update_sql = "UPDATE bookshelfs SET is_deleted = 0, ts = ? WHERE id = ?"
        local update_stmt = db:prepare(update_sql)
        if update_stmt then
            update_stmt:bind1(1, timestamp)
            update_stmt:bind1(2, bookshelf_id)
            update_stmt:step()
            update_stmt:close()
        end
        return bookshelf_id
    else
        -- Create new bookshelf
        local insert_sql = "INSERT INTO bookshelfs (name, is_deleted, ts, uuid) VALUES (?, 0, ?, NULL)"
        local insert_stmt = db:prepare(insert_sql)
        if not insert_stmt then return nil end
        
        insert_stmt:bind1(1, collection_name)
        insert_stmt:bind1(2, timestamp)
        
        if insert_stmt:step() == SQ3.DONE then
            local bookshelf_id = db:rowexec("SELECT last_insert_rowid()")
            insert_stmt:close()
            return bookshelf_id
        end
        insert_stmt:close()
    end
    return nil
end

function PocketBookDBHandler:findBookByPath(db, book_path, inbox_dir)
    local filename = book_path:match("/([^/]+)$")
    local folder = book_path:match("(.+)/[^/]+$")
    
    if not filename or not folder then
        logger.warn("Invalid book path format:", book_path)
        return nil
    end
    
    local full_folder = folder
    
    logger.dbg("Looking for book:", filename, "in folder:", full_folder)
    
    local sql = [[
        SELECT b.id FROM books_impl b
        JOIN files f ON b.id = f.book_id
        JOIN folders fo ON f.folder_id = fo.id
        WHERE f.filename = ? AND fo.name = ?
    ]]
    
    local stmt = db:prepare(sql)
    if not stmt then
        logger.warn("Failed to prepare book search query")
        return nil
    end
    
    stmt:bind1(1, filename)
    stmt:bind1(2, full_folder)
    local row = stmt:step()
    stmt:close()
    
    if type(row) == "table" then
        logger.dbg("Found book ID:", row[1])
        return row[1]
    else
        logger.warn("Book not found in database:", filename, "folder:", full_folder)
        return nil
    end
end

function PocketBookDBHandler:createBookshelfLink(db, bookshelf_id, book_id, timestamp)
    logger.dbg("Creating bookshelf link:", bookshelf_id, "->", book_id)
    
    -- Check if link exists
    local check_sql = "SELECT is_deleted FROM bookshelfs_books WHERE bookshelfid = ? AND bookid = ?"
    local check_stmt = db:prepare(check_sql)
    if not check_stmt then 
        logger.warn("Failed to prepare bookshelf link check query")
        return false
    end
    
    check_stmt:bind1(1, bookshelf_id)
    check_stmt:bind1(2, book_id)
    local existing = check_stmt:step()
    check_stmt:close()
    
    if type(existing) == "table" then
        logger.dbg("Reactivating existing bookshelf link")
        local update_sql = "UPDATE bookshelfs_books SET is_deleted = 0, ts = ? WHERE bookshelfid = ? AND bookid = ?"
        local update_stmt = db:prepare(update_sql)
        if update_stmt then
            update_stmt:bind1(1, timestamp)
            update_stmt:bind1(2, bookshelf_id)
            update_stmt:bind1(3, book_id)
            local result = update_stmt:step()
            update_stmt:close()
            if result == SQ3.DONE then
                logger.info("Successfully reactivated bookshelf link")
                return true
            else
                logger.warn("Failed to reactivate bookshelf link")
                return false
            end
        end
    else
        logger.dbg("Creating new bookshelf link")
        local insert_sql = "INSERT INTO bookshelfs_books (bookshelfid, bookid, ts, is_deleted) VALUES (?, ?, ?, 0)"
        local insert_stmt = db:prepare(insert_sql)
        if insert_stmt then
            insert_stmt:bind1(1, bookshelf_id)
            insert_stmt:bind1(2, book_id)
            insert_stmt:bind1(3, timestamp)
            local result = insert_stmt:step()
            insert_stmt:close()
            if result == SQ3.DONE then
                logger.info("Successfully created new bookshelf link")
                return true
            else
                logger.warn("Failed to create new bookshelf link")
                return false
            end
        else
            logger.warn("Failed to prepare bookshelf link insert query")
            return false
        end
    end
    return false
end

function PocketBookDBHandler:removeBookshelfLink(db, bookshelf_id, book_id, timestamp)
    local sql = "UPDATE bookshelfs_books SET is_deleted = 1, ts = ? WHERE bookshelfid = ? AND bookid = ?"
    local stmt = db:prepare(sql)
    if stmt then
        stmt:bind1(1, timestamp)
        stmt:bind1(2, bookshelf_id)
        stmt:bind1(3, book_id)
        stmt:step()
        stmt:close()
    end
end

function PocketBookDBHandler:removeBookshelf(db, collection_name, timestamp)
    local sql = "UPDATE bookshelfs SET is_deleted = 1, ts = ? WHERE name = ?"
    local stmt = db:prepare(sql)
    if stmt then
        stmt:bind1(1, timestamp)
        stmt:bind1(2, collection_name)
        stmt:step()
        stmt:close()
    end
end

function PocketBookDBHandler:updateBookMetadata(book_data, file_path)

    local read_status_column = G_reader_settings:readSetting("read_name")
    local read_date_column = G_reader_settings:readSetting("read_date_name")
    local favorite_column = G_reader_settings:readSetting("favorite_name")
    local collections_column = G_reader_settings:readSetting("collections_name")
    
    if not read_status_column and not read_date_column and not favorite_column and not collections_column then
        logger.dbg("No sync columns configured for PocketBook")
        return
    end

    local db = SQ3.open(db_path, "rw")
    if not db then
        logger.warn("Failed to open PocketBook database")
        return
    end
    
    db:exec("PRAGMA busy_timeout = 10000;")
    db:exec("BEGIN TRANSACTION")
    
    local success = true
    local book_id
    
    -- Find book_id by file path
    local filename = file_path:match("/([^/]+)$")
    local folder = file_path:match("(.+)/[^/]+$")
    
    if not filename or not folder then
        logger.warn("Invalid file path:", file_path)
        db:exec("ROLLBACK")
        db:close()
        return
    end
    
    local storage_id = get_storage_id(file_path)
    
    local find_book_sql = [[
        SELECT b.id
        FROM books_impl b
        JOIN files f ON b.id = f.book_id
        JOIN folders fo ON f.folder_id = fo.id
        WHERE f.filename = ? AND fo.name = ? AND f.storageid = ?;
    ]]
    
    local find_stmt = db:prepare(find_book_sql)
    if not find_stmt then
        logger.warn("Failed to prepare book search query")
        db:exec("ROLLBACK")
        db:close()
        return
    end
    
    find_stmt:bind1(1, filename)
    find_stmt:bind1(2, folder)
    find_stmt:bind1(3, storage_id)
    local book_row = find_stmt:step()
    find_stmt:close()
    
    if type(book_row) ~= "table" then
        logger.warn("Book not found in PocketBook database:", file_path)
        db:exec("ROLLBACK")
        db:close()
        return
    end
    
    book_id = book_row[1]
    
    -- Get current profile ID
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
    
    -- Parse metadata values
    local is_read = false
    local is_favorite = false
    local completed_timestamp = nil
    
    if read_status_column and book_data.user_metadata and book_data.user_metadata[read_status_column] then
        is_read = book_data.user_metadata[read_status_column]["#value#"] == true
    end
    
    if favorite_column and book_data.user_metadata and book_data.user_metadata[favorite_column] then
        is_favorite = book_data.user_metadata[favorite_column]["#value#"] == true
    end
    
    if read_date_column and book_data.user_metadata and book_data.user_metadata[read_date_column] then
        local date_value = book_data.user_metadata[read_date_column]["#value#"]
        
        if date_value and date_value ~= "" then
            -- Convert calibre datetime string to unix timestamp
            local year, month, day, hour, min, sec = date_value:match("(%d+)-(%d+)-(%d+)T(%d+):(%d+):(%d+)")
            
            if year and month and day and hour and min and sec then
                completed_timestamp = os.time({
                    year = tonumber(year),
                    month = tonumber(month),
                    day = tonumber(day),
                    hour = tonumber(hour),
                    min = tonumber(min),
                    sec = tonumber(sec)
                })
            end
        end
    end
    
    -- Check if settings record exists
    local select_settings_sql = [[
        SELECT bookid FROM books_settings 
        WHERE bookid = ? AND profileid = ?;
    ]]
    
    local select_settings_stmt = db:prepare(select_settings_sql)
    if not select_settings_stmt then
        logger.warn("Failed to prepare settings check query")
        success = false
    else
        select_settings_stmt:bind1(1, book_id)
        select_settings_stmt:bind1(2, profile_id)
        local settings_row = select_settings_stmt:step()
        select_settings_stmt:close()

        if type(settings_row) == "table" then
            -- Update existing settings
            local cpage_value = is_read and 100 or 0
            local npage_value = is_read and 100 or 0
            
            local update_settings_sql = [[
                UPDATE books_settings 
                SET completed = ?, favorite = ?, completed_ts = ?, cpage = ?, npage = ?
                WHERE bookid = ? AND profileid = ?;
            ]]
            
            local update_settings_stmt = db:prepare(update_settings_sql)
            if not update_settings_stmt then
                logger.warn("Failed to prepare settings update query")
                success = false
            else
                update_settings_stmt:bind1(1, is_read and 1 or 0)
                update_settings_stmt:bind1(2, is_favorite and 1 or 0)
                update_settings_stmt:bind1(3, completed_timestamp or 0)
                update_settings_stmt:bind1(4, cpage_value)
                update_settings_stmt:bind1(5, npage_value)
                update_settings_stmt:bind1(6, book_id)
                update_settings_stmt:bind1(7, profile_id)
                
                if update_settings_stmt:step() ~= SQ3.DONE then
                    logger.warn("Error updating PocketBook settings")
                    success = false
                else
                    logger.info("Updated PocketBook settings for book:", filename)
                end
                update_settings_stmt:close()
            end
        else
            -- Create new settings record
            local cpage_value = is_read and 100 or 0
            local npage_value = is_read and 100 or 0
            
            local insert_settings_sql = [[
                INSERT INTO books_settings (bookid, profileid, completed, favorite, completed_ts, cpage, npage)
                VALUES (?, ?, ?, ?, ?, ?, ?);
            ]]
            
            local insert_settings_stmt = db:prepare(insert_settings_sql)
            if not insert_settings_stmt then
                logger.warn("Failed to prepare settings insert query")
                success = false
            else
                insert_settings_stmt:bind1(1, book_id)
                insert_settings_stmt:bind1(2, profile_id)
                insert_settings_stmt:bind1(3, is_read and 1 or 0)
                insert_settings_stmt:bind1(4, is_favorite and 1 or 0)
                insert_settings_stmt:bind1(5, completed_timestamp or 0)
                insert_settings_stmt:bind1(6, cpage_value)
                insert_settings_stmt:bind1(7, npage_value)
                
                if insert_settings_stmt:step() ~= SQ3.DONE then
                    logger.warn("Error creating PocketBook settings")
                    success = false
                else
                    logger.info("Created PocketBook settings for book:", filename)
                end
                insert_settings_stmt:close()
            end
        end
    end
    
    if success then
        db:exec("COMMIT")
        db:exec("PRAGMA wal_checkpoint(FULL)")
        logger.info("PocketBook metadata update completed successfully")
    else
        db:exec("ROLLBACK")
        logger.warn("PocketBook metadata update failed, rolled back")
    end
    
    db:close()
end

return PocketBookDBHandler