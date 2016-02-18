<?php
namespace phil1895\AliyunMNS;

require_once(dirname(__FILE__) . '/mns/mns-autoloader.php');


use AliyunMNS\Client;
use AliyunMNS\Queue;
use AliyunMNS\Exception\MessageNotExistException;
use AliyunMNS\Model\QueueAttributes;
use AliyunMNS\Requests\ListQueueRequest;
use AliyunMNS\Requests\SendMessageRequest;
use AliyunMNS\Requests\CreateQueueRequest;
use AliyunMNS\Exception\MnsException;
use Yii;

/**
 * Class MnsQueue
 * 阿里云消息队列
 * @package common\components\alimns
 * @property Queue $queue This property is read-only.
 */
class MnsQueue
{
    // 队列为空时阻塞时间(s)
    const POLLING_WAITING_SECONDS = 30;
    // 消息被receive后下次可被再次消费的时间间隔
    const VISIBILITY_TIMEOUT = 30;

    private $client;

    public $queue;

    public function __construct()
    {
        $accessId = Yii::$app->params['mns.accessId'];
        $accessKey = Yii::$app->params['mns.accessKey'];
        $endPoint = Yii::$app->params['mns.isInternal'] ? Yii::$app->params['mns.endPointInternal'] : Yii::$app->params['mns.endPoint'];
        $this->client = new Client($endPoint, $accessId, $accessKey);
    }

    /**
     * 获取队列列表
     */
    public function listQueue()
    {
        $request = new ListQueueRequest();
        return $this->client->listQueue($request);
    }

    /**
     * 创建新队列
     */
    public function createQueue($queueName)
    {
        $queueAttributes = new QueueAttributes();
        $queueAttributes->setPollingWaitSeconds(self::POLLING_WAITING_SECONDS);
        $queueAttributes->setVisibilityTimeout(self::VISIBILITY_TIMEOUT);
        $request = new CreateQueueRequest($queueName, $queueAttributes);
        try {
            $this->client->createQueue($request);
        } catch (MnsException $e) {
            throw new \Exception('创建队列失败');
        }
    }

    /**
     * 获取一个队列
     */
    public function useQueue($queueName)
    {
        $this->queue = $this->client->getQueueRef($queueName);
    }

    /**
     * 获取队列中消息数量
     */
    public function getMessageCount()
    {
        $queueAttributes = $this->queue->getAttribute()->getQueueAttributes();

        return intval($queueAttributes->activeMessages);
    }

    /**
     * 添加消息
     */
    public function sendMessage($content)
    {
        $request = new SendMessageRequest($content);
        try {
            $this->queue->sendMessage($request);
        } catch (MnsException $e) {
            throw new \Exception('发送消息失败');
        }
    }

    /**
     * 获取消息
     */
    public function receiveMessage()
    {
        $receiptHandle = NULL;
        try {
            $res = $this->queue->receiveMessage();
            $message = [
                'id' => $res->getMessageId(),
                'body' => $res->getMessageBody(),
                'receiptHandle' => $res->getReceiptHandle(),
            ];
            return $message;
        } catch (MessageNotExistException $e) {
            return null;
        }
    }

    /**
     * 删除消息
     */
    public function deleteMessage($receiptHandle)
    {
        try {
            $this->queue->deleteMessage($receiptHandle);
        } catch (MnsException $e) {
            throw new \Exception('删除消息失败');
        }
    }

    /**
     * 获取并删除消息
     */
    public function receiveAndDeleteMessage()
    {
        $receiptHandle = NULL;
        try {
            $res = $this->queue->receiveMessage();
            $this->deleteMessage($res->getReceiptHandle());
            $message = [
                'id' => $res->getMessageId(),
                'body' => $res->getMessageBody(),
                'receiptHandle' => $res->getReceiptHandle(),
            ];

            return $message;
        } catch (MessageNotExistException $e) {
            return null;
        }
    }
}
?>
