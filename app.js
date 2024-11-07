// 导入所需的模块
const TelegramBot = require('node-telegram-bot-api');
const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');
const winston = require('winston');
const ffmpeg = require('fluent-ffmpeg');
const ffmpegPath = require('ffmpeg-static'); // ffmpeg 静态路径

// 设置 ffmpeg 路径
ffmpeg.setFfmpegPath(ffmpegPath);

// 加载环境变量
dotenv.config();

// 配置日志记录
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: './bot.log' }),
    new winston.transports.Console()
  ]
});

// 初始化 Telegram Bot
const bot = new TelegramBot(process.env.TELEGRAM_TOKEN, { polling: true });

// 创建数据库连接池
const db = mysql.createPool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME
});

// 数据库初始化
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

// 扫描目录中的文件
async function scanDirectory(directory, recursive) {
  const files = [];
  const entries = await fs.promises.readdir(directory, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(directory, entry.name);
    if (entry.isDirectory() && recursive) {
      files.push(...await scanDirectory(fullPath, recursive)); // 递归扫描子目录
    } else if (entry.isFile()) {
      const ext = path.extname(entry.name);
      if (process.env.ALLOWED_FILE_FORMATS.split(',').includes(ext)) {
        files.push({ folder: path.relative(process.env.SCAN_DIRECTORY, directory), name: entry.name, format: ext });
      }
    }
  }
  return files;
}

// 获取视频时长
async function getVideoDuration(filePath) {
  return new Promise((resolve, reject) => {
    ffmpeg.ffprobe(filePath, (err, metadata) => {
      if (err) return reject(err);
      resolve(metadata.format.duration); // 返回视频时长
    });
  });
}

// 更新数据库中的新文件
async function updateDatabaseWithNewFiles(files) {
  const now = new Date();
  for (const file of files) {
    const [rows] = await db.execute(
      "SELECT * FROM file WHERE folder_path = ? AND file_name = ?",
      [file.folder, file.name]
    );
    if (rows.length === 0) { // 如果文件不存在于数据库中
      await db.execute(
        "INSERT INTO file (scan_time, folder_path, file_name, file_format, is_uploaded) VALUES (?, ?, ?, ?, 0)",
        [now, file.folder, file.name, file.format]
      );
    }
  }
}

// 上传文件，带错误处理
let isUploading = false;
async function uploadFiles() {
  if (isUploading) return; // 如果正在上传，则返回
  isUploading = true;

  const [rows] = await db.execute(
    "SELECT * FROM file WHERE is_uploaded = 0 LIMIT ?",
    [parseInt(process.env.UPLOAD_COUNT)]
  );

  for (const row of rows) {
    await handleFileUpload(row); // 处理文件上传
  }

  isUploading = false;
}

// 处理单个文件上传
async function handleFileUpload(row) {
  const filePath = path.join(process.env.SCAN_DIRECTORY, row.folder_path, row.file_name);
  
  // 将 folder_path 按照斜杠进行分割，生成标签
  const tags = row.folder_path.split('/').map(dir => `#${dir}`).join(' '); // 生成标签
  const fileTag = `${tags}\n${path.basename(row.file_name, row.file_format)}`; // 合并标签和文件名

  let msg;
  const sendMessage = getSendMessageFunction(row.file_format); // 获取发送消息的函数

  try {
    let duration; // 视频时长

    if (['.mp4', '.avi'].includes(row.file_format)) {
      duration = await getVideoDuration(filePath);
    }

    // 带重试机制的上传
    msg = await retryTelegramRequest(() => sendMessage(filePath, fileTag, duration), filePath);

    if (!msg || !msg.message_id) {
      throw new Error(`发送消息失败，msg 响应: ${JSON.stringify(msg)}`);
    }

    let chatId = msg.chat.id.toString();
    if (chatId.startsWith('-100')) {
      chatId = chatId.slice(4); // 去除前缀
    }

    const messageLink = `https://t.me/c/${chatId}/${msg.message_id}`; // 消息链接
    logger.info(`文件上传成功，链接: ${messageLink}`);

    await db.execute(
      "UPDATE file SET is_uploaded = 1, upload_time = ?, file_link = ? WHERE id = ?",
      [new Date(), messageLink, row.id]
    );

    const [updatedRow] = await db.execute("SELECT * FROM file WHERE id = ?", [row.id]);
    const fileInfo = updatedRow[0];

    if (row.file_link === null) { // 如果文件链接为空，发送成功通知
      await notifySuccess(fileInfo);
    }

  } catch (err) {
    logger.error(`文件上传失败: ${filePath}, 错误: ${err.message}`);
  }
}

// 向群组发送成功通知
async function notifySuccess(fileInfo) {
  let messageContent = '文件上传成功:\n';
  for (const [key, value] of Object.entries(fileInfo)) {
    messageContent += `${key}: ${value}\n`; // 构建消息内容
  }

  await bot.sendMessage(process.env.GROUP_ID_2, messageContent); // 发送消息到指定群组
  logger.info(`通知已发送到群组 2: ${messageContent}`);
}

// 获取适当的发送消息函数
function getSendMessageFunction(fileFormat) {
  return (filePath, caption, duration) => {
    const options = {
      caption,
      supports_streaming: true // 设置视频为支持流媒体
    };

    if (['.mp4', '.avi'].includes(fileFormat) && duration) {
      options.duration = Math.floor(duration); // 设置视频时长
    }

    switch (fileFormat) {
      case '.jpg':
      case '.jpeg':
      case '.png':
        return bot.sendPhoto(process.env.GROUP_ID_1, filePath, options); // 发送照片
      case '.mp4':
      case '.avi':
        return bot.sendVideo(process.env.GROUP_ID_1, filePath, options); // 发送视频
      case '.pdf':
        return bot.sendDocument(process.env.GROUP_ID_1, filePath, options); // 发送文档
      default:
        throw new Error(`不支持的文件格式: ${fileFormat}`);
    }
  };
}

// 带重试机制的请求
async function retryTelegramRequest(requestFunc, filePath) {
  const maxRetries = 5; // 最大重试次数
  let retries = 0;

  while (retries < maxRetries) {
    try {
      return await requestFunc(); // 尝试请求
    } catch (err) {
      if (err.response && err.response.error_code === 429) { // 处理限速错误
        const retryAfter = err.response.parameters.retry_after || 5; // 获取重试间隔
        logger.warn(`上传速率限制: ${filePath}, ${retryAfter} 秒后重试.`);
        await new Promise(resolve => setTimeout(resolve, retryAfter * 1000)); // 等待重试
        retries += 1;
      } else {
        throw err;
      }
    }
  }
  throw new Error(`上传重试次数超限: ${filePath}`);
}

// 启动机器人并设置定时任务
async function startBot() {
  await initDatabase(); // 初始化数据库

  // 定时扫描目录任务
  setInterval(async () => {
    try {
      const files = await scanDirectory(process.env.SCAN_DIRECTORY, process.env.SCAN_RECURSIVE === 'true');
      await updateDatabaseWithNewFiles(files); // 更新数据库
    } catch (err) {
      logger.error(`目录扫描失败: ${err.message}`);
    }
  }, parseInt(process.env.SCAN_INTERVAL) * 1000);

  // 定时上传文件任务
  setInterval(async () => {
    try {
      await uploadFiles();
    } catch (err) {
      logger.error(`文件上传失败: ${err.message}`);
    }
  }, parseInt(process.env.UPLOAD_INTERVAL) * 1000);
}

// 启动机器人
startBot().catch(err => logger.error(`机器人启动失败: ${err.message}`));
