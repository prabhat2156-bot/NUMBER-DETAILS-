<?php
// Simple Telegram OSINT Bot
$botToken = '8348318598:AAFXCOUEqq7LuMZrstcZUL-NjzxPiZlPzK0';

$WELCOME_TEXT = "╔══════════════════════════════════╗
║          PHONE LOOKUP            ║
╠══════════════════════════════════╣
║ • Phone Number Information        ║
╚══════════════════════════════════╝";

// Phone Number API
$PHONE_API = 'https://ashuapi.ashupanel.online/api/gateway.php?key=sevenday&number=';

// File to store user input states
$USER_INPUT_STATES_FILE = 'user_input_states.json';

// ---------- File Initialization ----------
if (!file_exists($USER_INPUT_STATES_FILE)) {
    file_put_contents($USER_INPUT_STATES_FILE, json_encode([], JSON_PRETTY_PRINT));
    chmod($USER_INPUT_STATES_FILE, 0666);
}

// ---------- Telegram API Helper ----------
function tg($method, $params) {
    global $botToken;
    $url = "https://api.telegram.org/bot{$botToken}/{$method}";
    $ch  = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST           => true,
        CURLOPT_POSTFIELDS     => $params,
        CURLOPT_TIMEOUT        => 30,
        CURLOPT_CONNECTTIMEOUT => 10
    ]);
    $res = curl_exec($ch);
    curl_close($ch);
    return json_decode($res, true);
}

// ---------- Keyboard Layouts ----------
function mainKeyboard() {
    return [
        'keyboard' => [
            [
                ['text'=>'📱 Phone Number']
            ]
        ],
        'resize_keyboard'   => true,
        'one_time_keyboard' => false
    ];
}

function cancelKeyboard() {
    return [
        'keyboard' => [
            [
                ['text'=>'🚫 Cancel']
            ]
        ],
        'resize_keyboard' => true,
        'one_time_keyboard' => true
    ];
}

// ---------- User Input State Management ----------
function loadUserInputStates() {
    global $USER_INPUT_STATES_FILE;
    if (file_exists($USER_INPUT_STATES_FILE)) {
        return json_decode(file_get_contents($USER_INPUT_STATES_FILE), true) ?: [];
    }
    return [];
}

function saveUserInputStates($states) {
    global $USER_INPUT_STATES_FILE;
    file_put_contents($USER_INPUT_STATES_FILE, json_encode($states, JSON_PRETTY_PRINT));
}

function setUserInputState($userId, $state) {
    $states = loadUserInputStates();
    $states[$userId] = $state;
    saveUserInputStates($states);
}

function removeUserInputState($userId) {
    $states = loadUserInputStates();
    if (isset($states[$userId])) {
        unset($states[$userId]);
        saveUserInputStates($states);
    }
}

function getUserInputState($userId) {
    $states = loadUserInputStates();
    return isset($states[$userId]) ? $states[$userId] : null;
}

// ---------- API Call Function ----------
function makeApiCall($url, $timeout = 15) {
    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => $url,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => $timeout,
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_SSL_VERIFYHOST => false,
        CURLOPT_USERAGENT => 'Mozilla/5.0 (compatible; TelegramBot/1.0)',
        CURLOPT_HTTPHEADER => [
            'Accept: application/json, text/plain, */*',
            'Accept-Language: en-US,en;q=0.5',
            'Cache-Control: no-cache'
        ]
    ]);
    
    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error = curl_error($ch);
    $errno = curl_errno($ch);
    curl_close($ch);
    
    if ($errno === CURLE_OPERATION_TIMEDOUT) {
        return ['error' => 'timeout', 'message' => 'Request timed out'];
    }
    
    if ($response === false) {
        return ['error' => 'curl_error', 'message' => $error];
    }
    
    if ($httpCode !== 200) {
        return ['error' => 'http_error', 'message' => "HTTP $httpCode", 'response' => substr($response, 0, 200)];
    }
    
    return $response;
}

// ---------- Remove Credit Text ----------
function removeCreditText($text) {
    $creditPatterns = [
        '/t\.me\/[^\s]+/i',
        '/telegram\.me\/[^\s]+/i',
        '/credit\s*:\s*[^\n]+/i',
        '/developer\s*:\s*[^\n]+/i',
        '/@[^\s]+/i',
        '/https?:\/\/[^\s]+/i',
        '/credits\s*:\s*[^\n]+/i',
        '/API BY[^\n]+/i',
        '/Owner[^\n]+/i'
    ];
    
    foreach ($creditPatterns as $pattern) {
        $text = preg_replace($pattern, '', $text);
    }
    
    $text = preg_replace('/\n{3,}/', "\n\n", $text);
    $text = trim($text);
    
    return $text;
}

// ---------- Phone Number Processing ----------
function processPhoneNumber($phone) {
    global $PHONE_API;
    $apiUrl = $PHONE_API . urlencode($phone);
    
    $raw = makeApiCall($apiUrl, 15);
    
    if (is_array($raw) && isset($raw['error'])) {
        return "❌ <b>Error Fetching Data</b>\n\n📱 Number: " . htmlspecialchars($phone) . "\n\nError: " . $raw['error'];
    }
    
    $json = json_decode($raw, true);
    
    if (!$json || json_last_error() !== JSON_ERROR_NONE) {
        return "❌ <b>Invalid Response</b>\n\n📱 Number: " . htmlspecialchars($phone) . "\n\nCould not parse API response.";
    }
    
    if (!isset($json['success']) || $json['success'] !== true || !isset($json['result'])) {
        return "❌ <b>No Information Found</b>\n\n📱 Number: " . htmlspecialchars($phone);
    }
    
    $results = $json['result'];
    
    if (empty($results)) {
        return "❌ <b>No Information Found</b>\n\n📱 Number: " . htmlspecialchars($phone);
    }
    
    $formatted = "📱 <b>PHONE NUMBER INFORMATION</b>\n\n";
    $formatted .= "🔢 <b>Phone Number:</b> " . htmlspecialchars($phone) . "\n";
    $formatted .= "📊 <b>Records Found:</b> " . count($results) . "\n";
    $formatted .= "━━━━━━━━━━━━━━━━━━━━\n\n";
    
    foreach ($results as $index => $entry) {
        $entryNumber = $index + 1;
        
        $formatted .= "<b>📌 Record $entryNumber:</b>\n";
        $formatted .= "├─ 👤 <b>Name:</b> " . ($entry['name'] ?? 'N/A') . "\n";
        $formatted .= "├─ 👨 <b>Father:</b> " . ($entry['father_name'] ?? 'N/A') . "\n";
        $formatted .= "├─ 🏠 <b>Address:</b> " . ($entry['address'] ?? 'N/A') . "\n";
        $formatted .= "├─ 📡 <b>Circle/Sim:</b> " . ($entry['circle/sim'] ?? $entry['circle'] ?? 'N/A') . "\n";
        $formatted .= "├─ 📱 <b>Alt Mobile:</b> " . ($entry['alternative_mobile'] ?? 'N/A') . "\n";
        $formatted .= "├─ 🆔 <b>Aadhar:</b> " . ($entry['aadhar_number'] ?? $entry['id_number'] ?? 'N/A') . "\n";
        $formatted .= "└─ 📧 <b>Email:</b> " . ($entry['email'] ?? 'N/A') . "\n\n";
        
        if ($entryNumber < count($results)) {
            $formatted .= "━━━━━━━━━━━━━━━━━━━━\n\n";
        }
    }
    
    $formatted = removeCreditText($formatted);
    return $formatted;
}

// ---------- Send Loading Message ----------
function sendLoadingMessage($chatId, $text, $replyToMessageId = null) {
    $params = [
        'chat_id' => $chatId,
        'text' => $text,
        'parse_mode' => 'HTML'
    ];
    
    if ($replyToMessageId) {
        $params['reply_to_message_id'] = $replyToMessageId;
    }
    
    return tg('sendMessage', $params);
}

// ---------- Edit Message ----------
function editMessage($chatId, $messageId, $text) {
    return tg('editMessageText', [
        'chat_id' => $chatId,
        'message_id' => $messageId,
        'text' => $text,
        'parse_mode' => 'HTML'
    ]);
}

// ---------- Main Bot Handler ----------
$update = json_decode(file_get_contents('php://input'), true);

if (!$update && isset($_GET['test'])) {
    echo "Bot is working!";
    exit;
}

if ($update && isset($update['message'])) {
    $message = $update['message'];
    $chatId = $message['chat']['id'];
    $userId = $message['from']['id'] ?? null;
    $text = $message['text'] ?? '';
    $messageId = $message['message_id'];
    $chatType = $message['chat']['type'];
    
    // Handle /start command
    if (strpos($text, '/start') === 0) {
        tg('sendMessage', [
            'chat_id' => $chatId,
            'text' => $WELCOME_TEXT,
            'parse_mode' => 'HTML',
            'reply_markup' => json_encode(mainKeyboard())
        ]);
        exit;
    }
    
    // Handle button commands
    $lowerText = mb_strtolower($text, 'UTF-8');
    
    if ($lowerText == '📱 phone number') {
        setUserInputState($userId, 'awaiting_phone');
        tg('sendMessage', [
            'chat_id' => $chatId,
            'text' => "📱 <b>Enter Phone Number</b>\n\nSend 10-digit mobile number\n\n<b>Example:</b> <code>9876543210</code>",
            'parse_mode' => 'HTML',
            'reply_markup' => json_encode(cancelKeyboard())
        ]);
        exit;
    }
    
    if ($lowerText == '🚫 cancel') {
        removeUserInputState($userId);
        tg('sendMessage', [
            'chat_id' => $chatId,
            'text' => "✅ Operation cancelled",
            'reply_markup' => json_encode(mainKeyboard())
        ]);
        exit;
    }
    
    // Handle user input based on state
    $userState = getUserInputState($userId);
    
    if ($userState) {
        $loadingMsg = sendLoadingMessage($chatId, "🔍 Processing your request...", $messageId);
        
        if ($loadingMsg && $loadingMsg['ok']) {
            $loadingMsgId = $loadingMsg['result']['message_id'];
            
            if ($userState == 'awaiting_phone') {
                // Clean number
                $cleanNumber = preg_replace('/[^0-9]/', '', trim($text));
                
                // Validate Indian mobile number
                if (preg_match('/^[6-9]\d{9}$/', $cleanNumber)) {
                    $result = processPhoneNumber($cleanNumber);
                    editMessage($chatId, $loadingMsgId, $result);
                } else {
                    editMessage($chatId, $loadingMsgId, "❌ <b>Invalid Phone Number</b>\n\nPlease enter valid 10-digit Indian mobile number");
                }
            }
        }
        
        removeUserInputState($userId);
        exit;
    }
    
    // If no state and not a command, show menu
    tg('sendMessage', [
        'chat_id' => $chatId,
        'text' => $WELCOME_TEXT . "\n\nChoose option:",
        'parse_mode' => 'HTML',
        'reply_markup' => json_encode(mainKeyboard())
    ]);
    exit;
}

// Test endpoint
if (isset($_GET['test'])) {
    echo "Bot is working!";
}
?>
