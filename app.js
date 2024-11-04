// 导入所需模块
const TelegramBot = require('node-telegram-bot-api');
const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');
const winston = require('winston');

// 加载环境变量
dotenv.config();

// 创建日志记录器
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: './bot.log' }),
    new winston.transports.Console() // 可选: 也输出到控制台
  ]
});

// 创建 Telegram Bot 实例
const bot = new TelegramBot(process.env.TELEGRAM_TOKEN, { polling: true });

// 创建数据库连接
const db = mysql.createPool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME
});

// 数据库初始化和表创建
async function initDatabase() {
  await db.execute(`
    CREATE TABLE IF NOT EXISTS file (
      id INT AUTO_INCREMENT PRIMARY KEY,
      scan_time DATETIME COMMENT '扫描时间',
      folder_path VARCHAR(255) COMMENT '文件夹路径',
      file_name VARCHAR(255) COMMENT '文件名',
      file_format VARCHAR(50) COMMENT '文件格式',
      is_uploaded TINYINT(1) DEFAULT 0 COMMENT '是否已上传',
      upload_time DATETIME COMMENT '上传成功时间',
      file_link TEXT COMMENT '文件链接'
    )
  `);
}

// 扫描文件夹
async function scanDirectory(directory, recursive) {
  const files = [];
  const entries = await fs.promises.readdir(directory, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(directory, entry.name);
    if (entry.isDirectory() && recursive) {
      //递归处理文件夹
      files.push(...await scanDirectory(fullPath, recursive));
    } else if (entry.isFile()) {
      const ext = path.extname(entry.name);
      if (process.env.ALLOWED_FILE_FORMATS.split(',').includes(ext)) {
        // 收集允许的文件
        files.push({ folder: path.relative(process.env.SCAN_DIRECTORY, directory), name: entry.name, format: ext });
      }
    }
  }
  return files;
}

// 将扫描到的文件插入数据库
async function updateDatabaseWithNewFiles(files) {
  const now = new Date();
  for (const file of files) {
    const [rows] = await db.execute(
      "SELECT * FROM file WHERE folder_path = ? AND file_name = ?",
      [file.folder, file.name]
    );
    if (rows.length === 0) {
      await db.execute(
        "INSERT INTO file (scan_time, folder_path, file_name, file_format, is_uploaded) VALUES (?, ?, ?, ?, 0)",
        [now, file.folder, file.name, file.format]
      );
    }
  }
}

// 上传文件到 Telegram 群组，处理请求速率限制和状态
let isUploading = false;  // 上传进程状态

async function uploadFiles() {
  if (isUploading) return; // 如果正在上传，直接返回
  isUploading = true; // 设置为正在上传状态

  // 从数据库中选择未上传的文件
  const [rows] = await db.execute(
    "SELECT * FROM file WHERE is_uploaded = 0 LIMIT ?",
    [parseInt(process.env.UPLOAD_COUNT)]
  );

  for (const row of rows) {
    const filePath = path.join(process.env.SCAN_DIRECTORY, row.folder_path, row.file_name);
    const fileTag = `#${row.folder_path.replace(/\//g, ' ')}\n${row.file_name}`;

    let msg;
    const sendMessage = getSendMessageFunction(row.file_format);

    try {
      // 使用重试机制发送消息
      msg = await retryTelegramRequest(() => sendMessage(filePath, fileTag), filePath);

      // 检查 msg 是否有效
      if (!msg || !msg.message_id) {
        throw new Error(`未能成功发送消息，msg 返回值: ${JSON.stringify(msg)}`);
      }

      // 在文件成功上传后更新数据库
      const messageLink = `https://t.me/c/${process.env.GROUP_ID_1.split('-')[1]}/${msg.message_id}`;
      await db.execute(
        "UPDATE file SET is_uploaded = 1, upload_time = ?, file_link = ? WHERE id = ?",
        [new Date(), messageLink, row.id]
      );

      // 重新查询该行以获取更新后的值
      const [updatedRow] = await db.execute("SELECT * FROM file WHERE id = ?", [row.id]);
      const fileInfo = updatedRow[0];

      // 检查 file_link 字段是否从空更新成有值
      if (row.file_link === null) {
        const [updatedRow] = await db.execute("SELECT * FROM file WHERE id = ?", [row.id]);
        const fileInfo = updatedRow[0];

        // 将该行的内容逐行发送到群组 ID 2
        let messageContent = `文件上传成功:\n`;
        for (const [key, value] of Object.entries(fileInfo)) {
          messageContent += `${key}: ${value}\n`;
        }

        await bot.sendMessage(process.env.GROUP_ID_2, messageContent);
        logger.info(`通知已发送到群组2: ${messageContent}`);
      }

    } catch (err) {
      logger.error(`上传文件失败: ${filePath}, 错误: ${err.message}`);
    }
  }

  isUploading = false; // 恢复上传状态
}


// 根据文件格式返回发送消息的函数
function getSendMessageFunction(fileFormat) {
  switch (fileFormat) {
    case '.jpg':
    case '.jpeg':
    case '.png':
      return (filePath, caption) => bot.sendPhoto(process.env.GROUP_ID_1, filePath, { caption });
    case '.mp4':
    case '.avi':
      return (filePath, caption) => bot.sendVideo(process.env.GROUP_ID_1, filePath, { caption });
    case '.pdf':
      return (filePath, caption) => bot.sendDocument(process.env.GROUP_ID_1, filePath, { caption });
    // 可以根据需求增加更多文件格式的判断
    default:
      throw new Error(`不支持的文件格式: ${fileFormat}`);
  }
}

// 请求 Telegram API 时的重试逻辑
async function retryTelegramRequest(requestFunc, filePath) {
  const maxRetries = 5; // 最大重试次数
  let retries = 0;
  
  while (retries < maxRetries) {
    try {
      return await requestFunc(); // 尝试执行请求
    } catch (err) {
      if (err.response && err.response.error_code === 429) { // 请求频率限制
        const retryAfter = err.response.parameters.retry_after || 5;
        logger.warn(`上传文件速率受限: ${filePath}, 等待 ${retryAfter} 秒后重试.`);
        await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
        retries += 1;
      } else {
        throw err; // 非 429 错误直接抛出
      }
    }
  }
  throw new Error(`文件上传重试超出限制: ${filePath}`);
}

// 定时扫描与上传
async function startBot() {
  await initDatabase();

  setInterval(async () => {
    try {
      const files = await scanDirectory(process.env.SCAN_DIRECTORY, process.env.SCAN_RECURSIVE === 'true');
      await updateDatabaseWithNewFiles(files);
    } catch (err) {
      logger.error(`扫描文件夹失败: ${err.message}`);
    }
  }, parseInt(process.env.SCAN_INTERVAL) * 1000);

  setInterval(async () => {
    try {
      await uploadFiles();
    } catch (err) {
      logger.error(`上传文件失败: ${err.message}`);
    }
  }, parseInt(process.env.UPLOAD_INTERVAL) * 1000);
}

// 启动机器人
startBot().catch(err => logger.error(`Bot 启动失败: ${err.message}`));
