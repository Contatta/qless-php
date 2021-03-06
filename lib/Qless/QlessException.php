<?php

namespace Qless;

/**
 * The base class for all qless exceptions
 */
class QlessException extends \Exception
{
    /**
     * @var string
     */
    private $area;

    public function __construct($message, $area = null, $code = 0, \Exception $previous = null) {
        parent::__construct($message, $code, $previous);
        $this->area = $area;
    }

    /**
     * @param string $error
     *
     * @return QlessException
     */
    public static function createExceptionFromError($error) {
        if (preg_match('/^ERR.*user_script:\d+:\s*(?<area>[\w.]+)\(\):\s*(?<message>.*)/', $error, $matches) > 0) {
            $area    = $matches['area'];
            $message = $matches['message'];
        } else {
            $area    = null;
            $message = $error;
        }

        switch (true) {
        case (stripos($message, 'does not exist') !== false):
            return new InvalidJobException($message, $area);

        case (stripos($message, 'given out to another worker') !== false):
            return new JobLostException($message, $area);

        case (stripos($message, 'not currently running') !== false):
        default:
            return new QlessException($message, $area);
        }
    }

    public function getArea() {
        return $this->area;
    }
}

class InvalidJobException extends QlessException
{
}

class JobLostException extends QlessException
{
}