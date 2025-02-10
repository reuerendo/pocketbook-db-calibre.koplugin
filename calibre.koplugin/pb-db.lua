--[[
	This module adds books transferred using the "smart device app" protocol of the calibre to the Pocketbook database
--]]

local logger = require("logger")
local lfs = require("libs/libkoreader-lfs")
local SQ3 = require("lua-ljsqlite3/init")
local current_timestamp = os.time()

local PocketBookDBHandler = {}

local function get_storage_id(filename)
    if string.match(filename, "^/mnt/ext1") then
        return 1
    else
        return 2
    end
end

function PocketBookDBHandler:saveBookToDatabase(arg, filename, collections_lookup_name)
    local db_path = "/mnt/ext1/system/explorer-3/explorer-3.db"
    
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
        logger.info("Ошибка: не удалось открыть базу данных")
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
    logger.info("Транзакция начата")
    
	logger.info("FILENAME:", filename)
    local success = true
    local folder_id, book_id
    local folder = filename:match("(.+)/[^/]+$")
    local storage_id = get_storage_id(filename)
		logger.info("stotage_id:", storage_id)
    local file_name = filename:match("/([^/]+)$")
    local file_ext = file_name:match("%.([^%.]+)$")

    if not arg.metadata or not arg.metadata.title or arg.metadata.title == "" then
        logger.info("Ошибка: отсутствуют метаданные или название книги")
        db:exec("ROLLBACK")
        safe_db_close()
        return
    end

    local file_stats = lfs.attributes(filename)
    if not file_stats then
        logger.info("Ошибка: не удалось получить атрибуты файла")
        db:exec("ROLLBACK")
        safe_db_close()
        return
    end

    if not folder or folder == "" then
        logger.dbg("Ошибка: `folder` пустой или nil! Полный путь файла: " .. filename)
        db:exec("ROLLBACK")
        safe_db_close()
        return
    end

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
        logger.info("Ошибка: не удалось подготовить SQL-запрос для папки!")
        db:exec("ROLLBACK")
        safe_db_close()
        return
    end

    folder_stmt:bind(storage_id, folder)
    folder_stmt:step()
    folder_stmt:close()

    local select_stmt = db:prepare(select_folder_sql)
    if not select_stmt then
        logger.info("Ошибка: не удалось подготовить SELECT-запрос!")
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
        logger.info("Ошибка: не удалось получить ID папки")
        db:exec("ROLLBACK")
        safe_db_close()
        return
    end

    local author = table.concat(arg.metadata.authors or {}, ", ")
    local first_author = arg.metadata.authors and arg.metadata.authors[1] or ""
	
	-- Проверяем существование книги
	local check_book_sql = [[
		SELECT id FROM books_impl 
		WHERE title = ? AND author = ?;
	]]

	local check_book_stmt = db:prepare(check_book_sql)
	check_book_stmt:bind1(1, title)
	check_book_stmt:bind1(2, author)
	local existing_book = check_book_stmt:step()
	check_book_stmt:close()

	if type(existing_book) == "table" then
		-- Обновляем существующую книгу
		book_id = existing_book[1]
		local update_book_sql = [[
			UPDATE books_impl SET
				series = ?,
				numinseries = ?,
				size = ?,
				isbn = ?,
				updated = ?,
				ts_added = ?
			WHERE id = ?;
		]]
		
		local update_book_stmt = db:prepare(update_book_sql)
		update_book_stmt:bind1(1, safeToString(arg.metadata.series))
		update_book_stmt:bind1(2, tonumber(safeToString(arg.metadata.series_index)) or 0)
		update_book_stmt:bind1(3, arg.metadata.size)
		update_book_stmt:bind1(4, safeToString(arg.metadata.isbn))
		update_book_stmt:bind1(5, current_timestamp)
		update_book_stmt:bind1(6, current_timestamp)
		update_book_stmt:bind1(7, book_id)

		if update_book_stmt:step() ~= SQ3.DONE then
			logger.info("Ошибка при обновлении книги")
			db:exec("ROLLBACK")
			safe_db_close()
			return
		end
		update_book_stmt:close()
	else
	
		-- Добавляем новую книгу (оставляем оригинальный код вставки)
    
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
        logger.info("Ошибка: не удалось подготовить SQL-запрос для книги!")
        db:exec("ROLLBACK")
        safe_db_close()
        return
    end

    local title = safeToString(arg.metadata.title)
    
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
        logger.info("Ошибка при добавлении книги")
        db:exec("ROLLBACK")
        safe_db_close()
        return
    end

    book_id = db:rowexec("SELECT last_insert_rowid()")
    book_stmt:close()

    if not book_id then
        logger.info("Ошибка: не удалось получить ID книги")
        db:exec("ROLLBACK")
        safe_db_close()
        return
    end
	end
	
	-- Проверяем существование файла
	local check_file_sql = [[
		SELECT id FROM files 
		WHERE filename = ? AND folder_id = ?;
	]]

	local check_file_stmt = db:prepare(check_file_sql)
	check_file_stmt:bind1(1, file_name)
	check_file_stmt:bind1(2, folder_id)
	local existing_file = check_file_stmt:step()
	check_file_stmt:close()

	if type(existing_file) == "table" then
		-- Обновляем существующий файл
		local update_file_sql = [[
			UPDATE files SET
				book_id = ?,
				size = ?,
				modification_time = ?
			WHERE filename = ? AND folder_id = ?;
		]]
		
		local update_file_stmt = db:prepare(update_file_sql)
		update_file_stmt:bind1(1, book_id)
		update_file_stmt:bind1(2, file_stats.size)
		update_file_stmt:bind1(3, file_stats.modification)
		update_file_stmt:bind1(4, file_name)
		update_file_stmt:bind1(5, folder_id)

		if update_file_stmt:step() ~= SQ3.DONE then
			logger.info("Ошибка при обновлении файла")
			success = false
		end
		update_file_stmt:close()
	else
	
		-- Добавляем новый файл (оставляем оригинальный код вставки)
	
    local files_sql = [[
        INSERT INTO files (
            storageid, folder_id, book_id, filename, 
            size, modification_time, ext
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ]]
    
    local files_stmt = db:prepare(files_sql)
    if not files_stmt then
        logger.info("Ошибка: не удалось подготовить SQL-запрос для файла!")
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
        logger.info("Ошибка при добавлении файла")
        success = false
    end
    files_stmt:close()
	
	if success and arg.metadata.user_metadata[collections_lookup_name] and arg.metadata.user_metadata[collections_lookup_name]["#value#"] then
		local collections = arg.metadata.user_metadata[collections_lookup_name]["#value#"]
        
        for _, collection_name in ipairs(collections) do
            local select_bookshelf_sql = [[
                SELECT id FROM bookshelfs 
                WHERE name = ?;
            ]]
            
			local select_bookshelf_stmt = db:prepare(select_bookshelf_sql)
			if not select_bookshelf_stmt then
				logger.info("Ошибка: не удалось подготовить SQL-запрос для проверки полки!")
				success = false
				break
			end

			select_bookshelf_stmt:bind1(1, collection_name)
			local bookshelf_row = select_bookshelf_stmt:step()
			select_bookshelf_stmt:close()

			local bookshelf_id
			if type(bookshelf_row) == "table" then
				bookshelf_id = bookshelf_row[1]
				
				-- Добавляем UPDATE запрос для обновления is_deleted
				local update_sql = [[
					UPDATE bookshelfs 
					SET is_deleted = 0 
					WHERE rowid = ?;
				]]
				
				local update_stmt = db:prepare(update_sql)
				if not update_stmt then
					logger.info("Ошибка: не удалось подготовить SQL-запрос для обновления is_deleted!")
					success = false
					break
				end
				
				update_stmt:bind1(1, bookshelf_id)
				if update_stmt:step() ~= SQ3.DONE then
					logger.info("Ошибка при обновлении is_deleted")
					success = false
					break
				end
				update_stmt:close()
			else
				local insert_bookshelf_sql = [[
					INSERT INTO bookshelfs (name, is_deleted, ts, uuid)
					VALUES (?, 0, ?, NULL);
				]]
				
				local insert_bookshelf_stmt = db:prepare(insert_bookshelf_sql)
				if not insert_bookshelf_stmt then
					logger.info("Ошибка: не удалось подготовить SQL-запрос для создания полки!")
					success = false
					break
				end
				
				insert_bookshelf_stmt:bind1(1, collection_name)
				insert_bookshelf_stmt:bind1(2, current_timestamp)
				
				if insert_bookshelf_stmt:step() ~= SQ3.DONE then
					logger.info("Ошибка при создании полки")
					success = false
					break
				end
				
				bookshelf_id = db:rowexec("SELECT last_insert_rowid()")
				insert_bookshelf_stmt:close()
			end
            
            if bookshelf_id then
                local insert_bookshelf_book_sql = [[
                    INSERT INTO bookshelfs_books (bookshelfid, bookid, ts, is_deleted)
                    VALUES (?, ?, ?, 0);
                ]]
                
                local insert_bookshelf_book_stmt = db:prepare(insert_bookshelf_book_sql)
                if not insert_bookshelf_book_stmt then
                    logger.info("Ошибка: не удалось подготовить SQL-запрос для связи книги с полкой!")
                    success = false
                    break
                end
                
                insert_bookshelf_book_stmt:bind1(1, bookshelf_id)
                insert_bookshelf_book_stmt:bind1(2, book_id)
                insert_bookshelf_book_stmt:bind1(3, current_timestamp)
                
                if insert_bookshelf_book_stmt:step() ~= SQ3.DONE then
                    logger.info("Ошибка при создании связи книги с полкой")
                    success = false
                    break
                end
                insert_bookshelf_book_stmt:close()
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

return PocketBookDBHandler
