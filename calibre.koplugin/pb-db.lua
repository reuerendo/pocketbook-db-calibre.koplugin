--[[
	This module adds books transferred using the "smart device app" protocol of the calibre to the Pocketbook database
--]]

local logger = require("logger")
local lfs = require("libs/libkoreader-lfs")
local SQ3 = require("lua-ljsqlite3/init")
local ffi = require("ffi")
local inkview = ffi.load("inkview")
local current_timestamp = os.time()

local PocketBookDBHandler = {}

local function get_storage_id(filename)
    if string.match(filename, "^/mnt/ext1") then
        return 1
    else
        return 2
    end
end

function PocketBookDBHandler:saveBookToDatabase(arg, filename)
    local db_path = "/mnt/ext1/system/explorer-3/explorer-3.db"
    collections_lookup_name = G_reader_settings:readSetting("collections_name")
    read_lookup_name = G_reader_settings:readSetting("read_name")
	read_date_lookup_name = G_reader_settings:readSetting("read_date_name")
    favorite_lookup_name = G_reader_settings:readSetting("favorite_name")
    
    -- Добавляем отладочную информацию
    logger.info("=== Начало отладки метаданных ===")
    logger.info("Метаданные книги:", arg.metadata)
    
    if arg.metadata.user_metadata then
        logger.info("user_metadata присутствует")
        logger.info("Содержимое user_metadata:", arg.metadata.user_metadata)
        
        if arg.metadata.user_metadata[collections_lookup_name] then
            logger.info("Найдены данные для collections_lookup_name:", collections_lookup_name)
            logger.info("Содержимое:", arg.metadata.user_metadata[collections_lookup_name])
            
            if arg.metadata.user_metadata[collections_lookup_name]["#value#"] then
                logger.info("Найдено значение #value#:", 
                    arg.metadata.user_metadata[collections_lookup_name]["#value#"])
            else
                logger.info("Значение #value# отсутствует")
            end
        else
            logger.info("Данные для collections_lookup_name отсутствуют:", collections_lookup_name)
        end
    else
        logger.info("user_metadata отсутствует в метаданных")
    end
    logger.info("=== Конец отладки метаданных ===")
    
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
	logger.info("storage_id:", storage_id)
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

    -- Обработка папки
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
    local title = safeToString(arg.metadata.title)
    
	-- Сначала проверяем существование файла
	local check_file_sql = [[
		SELECT id, book_id FROM files 
		WHERE filename = ? AND folder_id = ?;
	]]

	local check_file_stmt = db:prepare(check_file_sql)
	if not check_file_stmt then
        logger.info("Ошибка: не удалось подготовить запрос проверки файла")
        db:exec("ROLLBACK")
        safe_db_close()
        return
    end
    
	check_file_stmt:bind1(1, file_name)
	check_file_stmt:bind1(2, folder_id)
	local existing_file = check_file_stmt:step()
	check_file_stmt:close()

	if type(existing_file) == "table" then
		-- Файл существует, получаем book_id
		local file_id = existing_file[1]
		book_id = existing_file[2]
		logger.info("Найден существующий файл, file_id:", file_id, "book_id:", book_id)
		
		-- Обновляем информацию о файле
		local update_file_sql = [[
			UPDATE files SET
				size = ?,
				modification_time = ?
			WHERE id = ?;
		]]
		
		local update_file_stmt = db:prepare(update_file_sql)
		if not update_file_stmt then
			logger.info("Ошибка: не удалось подготовить запрос обновления файла")
			db:exec("ROLLBACK")
			safe_db_close()
			return
		end
		
		update_file_stmt:bind1(1, file_stats.size)
		update_file_stmt:bind1(2, file_stats.modification)
		update_file_stmt:bind1(3, file_id)

		if update_file_stmt:step() ~= SQ3.DONE then
			logger.info("Ошибка при обновлении файла")
			success = false
		end
		update_file_stmt:close()
		
		-- Обновляем информацию о книге
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
			logger.info("Ошибка: не удалось подготовить запрос обновления книги")
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
			logger.info("Ошибка при обновлении книги")
			success = false
		end
		update_book_stmt:close()
		logger.info("Обновлена существующая книга, book_id:", book_id)
		
	else
		-- Файл не существует, создаем новую запись книги и файла
		logger.info("Файл не найден, создаем новые записи")
		
		-- Добавляем новую книгу
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
		logger.info("Создана новая книга, book_id:", book_id)
		
		-- Добавляем новый файл
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
		logger.info("Создан новый файл")
	end
	
	if success then
		-- Обработка коллекций
		if arg.metadata.user_metadata and 
		   arg.metadata.user_metadata[collections_lookup_name] and 
		   arg.metadata.user_metadata[collections_lookup_name]["#value#"] then
			local collections = arg.metadata.user_metadata[collections_lookup_name]["#value#"]
			logger.info("Начало обработки коллекций:", collections)
			
			for _, collection_name in ipairs(collections) do
				logger.info("Обработка коллекции:", collection_name)
				
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
					logger.info("Найдена существующая полка, id:", bookshelf_id)
					
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
					logger.info("Создание новой полки:", collection_name)
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
					logger.info("Создана новая полка, id:", bookshelf_id)
				end
				
				if bookshelf_id then
					-- Проверяем существующую связь
					local check_link_sql = [[
						SELECT 1 FROM bookshelfs_books 
						WHERE bookshelfid = ? AND bookid = ?;
					]]
					
					local check_link_stmt = db:prepare(check_link_sql)
					check_link_stmt:bind1(1, bookshelf_id)
					check_link_stmt:bind1(2, book_id)
					local existing_link = check_link_stmt:step()
					check_link_stmt:close()
					
					if type(existing_link) ~= "table" then
						logger.info("Создание связи книги с полкой")
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
						logger.info("Связь книги с полкой создана успешно")
					else
						logger.info("Связь книги с полкой уже существует")
					end
				end
			end
		end
	end

	if success then
		-- Проверяем наличие меток в метаданных
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
		logger.info("Профиль:", profile_id)
		
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
			logger.info("Raw date from Calibre:", date_value) -- Debug: original date from Calibre
			
			if date_value and date_value ~= "" then
				-- Convert calibre datetime string to YYYY-MM-DD format
				-- Calibre format: "YYYY-MM-DDTHH:MM:SS+00:00" (UTC)
				local year, month, day, hour, min, sec = date_value:match("(%d+)-(%d+)-(%d+)T(%d+):(%d+):(%d+)")
				logger.info("Parsed components:", year, month, day, hour, min, sec) -- Debug: parsed components
				
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
					logger.info("UTC timestamp:", utc_time) -- Debug: UTC timestamp
					
					-- Get local time
					local local_time = os.date("*t", utc_time)
					logger.info("Local time components:", local_time.year, local_time.month, local_time.day) -- Debug: local time components
					
					completed_date = string.format("%04d-%02d-%02d", 
						local_time.year, local_time.month, local_time.day)
					logger.info("Final converted date (UTC->Local):", completed_date)
				else
					logger.info("Failed to parse date components from:", date_value)
				end
			else
				logger.info("Date value is empty or nil")
			end
		else
			logger.info("Read date metadata not found or conditions not met")
		end
		
		-- Передаем данные в DocSettings вместо базы данных
		if has_read or has_favorite or completed_date then
			local DocSettings = require("docsettings")
			local doc_settings = DocSettings:open(filename)
			
			if has_read then
				logger.info("Устанавливаем метку 'прочитано' в DocSettings")
				doc_settings:saveSetting("summary", {status = "complete"})
			end
			
			if completed_date then
				logger.info("Устанавливаем дату завершения в DocSettings:", completed_date)
				doc_settings:saveSetting("summary", {status = "complete", modified = completed_date})
			end
			
			doc_settings:flush()
			logger.info("Метки сохранены в DocSettings")
		end
		
		-- Обрабатываем избранное в базе данных (если нужно)
        if has_read or has_favorite then
			local completed = has_read and 1 or 0
			local favorite = has_favorite and 1 or 0
            
            local select_settings_sql = [[
                SELECT bookid FROM books_settings 
                WHERE bookid = ? AND profileid = ?;
            ]]
            
            local select_settings_stmt = db:prepare(select_settings_sql)
            if not select_settings_stmt then
                logger.info("Ошибка: не удалось подготовить SQL-запрос для проверки настроек!")
                success = false
            else
                select_settings_stmt:bind1(1, book_id)
                select_settings_stmt:bind1(2, profile_id)
                local settings_row = select_settings_stmt:step()
                select_settings_stmt:close()

                if type(settings_row) == "table" then
                    -- Обновляем существующие настройки
                    local update_settings_sql = [[
                        UPDATE books_settings 
                        SET completed = ?, favorite = ?
                        WHERE bookid = ? AND profileid = ?;
                    ]]
                    
                    local update_settings_stmt = db:prepare(update_settings_sql)
                    if not update_settings_stmt then
                        logger.info("Ошибка: не удалось подготовить SQL-запрос для обновления настроек!")
                        success = false
                    else
                        update_settings_stmt:bind1(1, completed)
                        update_settings_stmt:bind1(2, favorite)
                        update_settings_stmt:bind1(3, book_id)
                        update_settings_stmt:bind1(4, profile_id)
                        
                        if update_settings_stmt:step() ~= SQ3.DONE then
                            logger.info("Ошибка при обновлении настроек")
                            success = false
                        end
                        update_settings_stmt:close()
                    end
                else
                    -- Создаем новую запись настроек
                    local insert_settings_sql = [[
                        INSERT INTO books_settings (bookid, profileid, completed, favorite)
                        VALUES (?, ?, ?, ?);
                    ]]
                    
                    local insert_settings_stmt = db:prepare(insert_settings_sql)
                    if not insert_settings_stmt then
                        logger.info("Ошибка: не удалось подготовить SQL-запрос для создания настроек!")
                        success = false
                    else
                        insert_settings_stmt:bind1(1, book_id)
                        insert_settings_stmt:bind1(2, profile_id)
                        insert_settings_stmt:bind1(3, completed)
                        insert_settings_stmt:bind1(4, favorite)
                        
                        if insert_settings_stmt:step() ~= SQ3.DONE then
                            logger.info("Ошибка при создании настроек")
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

return PocketBookDBHandler