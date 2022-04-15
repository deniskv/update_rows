<?php

require_once 'abstract.php';

class GoMage_Shell_WalletMigrationSS extends
    Mage_Shell_Abstract
{
    /** @var Mage_Core_Model_Resource */
    protected $_resource;
    
    /** @var Varien_Db_Adapter_Interface */
    protected $_connection;
    
    /**
     * @var array
     */
    protected $_sqlQueue = [];
    
    protected $_queueLimit = 5000; // Limit for part queries in queue;
    
    protected $_hasErrors = false;
    
    /**
     * @param $query
     * @return $this
     */
    protected function _addToDb($query)
    {
        $this->_sqlQueue[] = $query;
        
        if (count($this->_sqlQueue) >= $this->_queueLimit) {
            $this->_sendQueueToDb();
        }
        
        return $this;
    }
    
    /**
     * @return $this
     */
    protected function _sendQueueToDb()
    {
        if (count($this->_sqlQueue)) {
            $this->_connection->beginTransaction();
            
            try {
                foreach ($this->_sqlQueue as $sql) {
                    $this->_connection->query($sql);
                }
                
                $this->_connection->commit();
            } catch (Exception $e) {
                $this->_connection->rollBack();
                $this->_hasErrors = true;
            }
            
            $this->_sqlQueue = [];
        }
        
        return $this;
    }
    
    protected function _construct()
    {
        $this->_resource   = Mage::getSingleton('core/resource');
        $this->_connection = $this->_resource->getConnection('core_write');
        
        return parent::_construct();
    }
    
    protected function getLogPath()
    {
        return basename(__FILE__) . '.log';
    }
    
    /**
     * Collect data for migration
     *
     * @throws Exception
     *
     */
    protected function _collect()
    {
        $write = $this->_connection;
        
        $truncateTables = [
            'gomage_wallet_tmp',
            'gomage_wallet_credit_tmp',
            'gomage_wallet_debit_tmp',
        ];
        
        foreach ($truncateTables as $table) {
            if ($this->_connection->isTableExists($table)) {
                $this->_connection->truncateTable($table);
            }
        }

        $write->query('
                    CREATE TABLE IF NOT EXISTS `gomage_wallet_credit_tmp` (
                        `id` INT(11) UNSIGNED NOT NULL COMMENT \'Credit id\',
                        `wallet_id` INT(11) UNSIGNED NOT NULL COMMENT \'Wallet id\',
                        `wallet_event` INT(11) UNSIGNED NOT NULL COMMENT \'Wallet event\',
                        `wallet_event_id` INT(11) UNSIGNED NOT NULL COMMENT \'Wallet event id\',
                        `event_sum` DECIMAL(11,2) UNSIGNED NOT NULL COMMENT \'Event sum\',
                        `comment` TEXT NULL COMMENT \'Comment\',
                        `status` TINYINT(1) UNSIGNED NOT NULL COMMENT \'Event status\',
                        `created_at` TIMESTAMP NOT NULL COMMENT \'Created date\',
                        `updated_at` TIMESTAMP NOT NULL COMMENT \'Updated date\'
                    )
                    COLLATE=\'utf8_general_ci\';
                ');
        
        $write->query('
                    CREATE TABLE IF NOT EXISTS `gomage_wallet_debit_tmp` (
                        `id` INT(11) UNSIGNED NOT NULL COMMENT \'Debit id\',
                        `wallet_id` INT(11) UNSIGNED NOT NULL COMMENT \'Wallet id\',
                        `wallet_event` INT(11) UNSIGNED NOT NULL COMMENT \'Wallet event\',
                        `wallet_event_id` INT(11) UNSIGNED NOT NULL COMMENT \'Wallet event id\',
                        `event_sum` DECIMAL(11,2) UNSIGNED NOT NULL COMMENT \'Event sum\',
                        `comment` TEXT NULL COMMENT \'Comment\',
                        `status` TINYINT(1) UNSIGNED NOT NULL COMMENT \'Event status\',
                        `created_at` TIMESTAMP NOT NULL COMMENT \'Created date\',
                        `updated_at` TIMESTAMP NOT NULL COMMENT \'Updated date\'
                    )
                    COLLATE=\'utf8_general_ci\';
                ');
        
        
        $wallets       = Mage::getSingleton('gomage_wallet/wallet')->getCollection();
        $start         = microtime(1);
        $wi            = 0;
        $wallets_count = count($wallets);
        $ci            = 1;
        $di            = 100000;
        
        /** @var GoMage_Wallet_Model_Wallet $wallet */
        foreach ($wallets as $wallet) {
            $wi++;
            $c_start    = time();
            $customerId = $wallet->getCustomerId();
            
            $sales_collection = Mage::getModel('sales/order_item')
                ->getCollection();
            
            $sales_collection->getSelect()
                ->join(['orders_tbl' => 'sales_flat_order'], 'main_table.order_id = orders_tbl.entity_id', ['increment_id', 'status'])
                ->joinLeft(['offer_tbl' => 'offer'], 'main_table.offer_id = offer_tbl.id', ['number']);
            
            $sales_collection->addFieldToFilter('main_table.parent_item_id', ['null' => true]);
            $sales_collection->addFieldToFilter('designer_id', $customerId);
            $sales_collection->addFieldToFilter('orders_tbl.status', array('in' => array('complete', 'canceled')));
            $sales_collection->addFieldToFilter('main_table.offer_id', array('gt' => 0));
            
            foreach ($sales_collection as $item) {
                $this->_addToDb("
                            INSERT INTO `gomage_wallet_credit_tmp` VALUES($ci, " . $wallet->getId() . ", 4, '" . $item->getId() . "', '" . ($item->getOfferItemProfit() * $item->getQtyOrdered()) . "', '" . "Profit credit for Campaign #" . $item->getData('number') . " " . PHP_EOL . "Order #" . $item->getData('increment_id') . " " . PHP_EOL . "Qty " . (int) $item->getQtyOrdered() . "', 2, '" . $item->getData('created_at') . "', '" . $item->getData('updated_at') . "');
                        ");
                $ci++;
                
                if ((int) $item->getQtyCanceled() > 0) {
                    $this->_addToDb("
                                INSERT INTO `gomage_wallet_debit_tmp` VALUES($di, " . $wallet->getId() . ", 4, '" . $item->getId() . "', '" . ($item->getOfferItemProfit() * $item->getQtyCanceled()) . "', '" . "Refund profit for Campaign #" . $item->getData('number') . " " . PHP_EOL . "Order #" . $item->getData('increment_id') . " " . PHP_EOL . "Qty " . (int) $item->getQtyCanceled() . "', 2, '" . $item->getData('created_at') . "', '" . $item->getData('updated_at') . "');
                            ");
                    $di++;
                } else {
                    if ((int) $item->getQtyRefunded() > 0) {
                        $this->_addToDb("
                                    INSERT INTO `gomage_wallet_debit_tmp` VALUES($di, " . $wallet->getId() . ", 4, '" . $item->getId() . "', '" . ($item->getOfferItemProfit() * $item->getQtyRefunded()) . "', '" . "Refund profit for Campaign #" . $item->getData('number') . " " . PHP_EOL . "Order #" . $item->getData('increment_id') . " " . PHP_EOL . "Qty " . (int) $item->getQtyRefunded() . "', 2, '" . $item->getData('created_at') . "', '" . $item->getData('updated_at') . "');
                                ");
                        $di++;
                    }
                }
            }
            
            unset($sales_collection);
            
            $migrate_credits = Mage::getModel('gomage_wallet/credit')
                ->getCollection();
            
            $migrate_credits->addFieldToFilter('wallet_id', $wallet->getId());
            $migrate_credits->addFieldToFilter('wallet_event', array('in' => array(3, 5)));
            
            foreach ($migrate_credits as $credit) {
                $this->_addToDb("
                            INSERT INTO `gomage_wallet_credit_tmp` VALUES(" . $ci . ", " . $wallet->getId() . ", " . $credit->getData('wallet_event') . ", '" . $credit->getData('wallet_event_id') . "', '" . $credit->getData('event_sum') . "', '" . $credit->getData('comment') . "', " . $credit->getData('status') . ", '" . $credit->getData('created_at') . "', '" . $credit->getData('updated_at') . "');
                        ");
                $ci++;
            }
            
            unset($migrate_credits);
            
            $migrate_debits = Mage::getModel('gomage_wallet/debit')
                ->getCollection();
            
            $migrate_debits->addFieldToFilter('wallet_id', $wallet->getId());
            $migrate_debits->addFieldToFilter('comment', array('eq' => 'Dashboard refund request'));
            
            foreach ($migrate_debits as $debit) {
                $this->_addToDb("
                            INSERT INTO `gomage_wallet_debit_tmp` VALUES(" . $debit->getId() . ", " . $wallet->getId() . ", 2, '" . $debit->getData('wallet_event_id') . "', '" . $debit->getData('event_sum') . "', 'Dashboard refund request', " . $debit->getData('status') . ", '" . $debit->getData('created_at') . "', '" . $debit->getData('updated_at') . "');
                        ");
            }
            
            unset($migrate_debits);
            
            $migrate_debits = Mage::getModel('gomage_wallet/debit')
                ->getCollection();
            
            $migrate_debits->addFieldToFilter('wallet_id', $wallet->getId());
            $migrate_debits->addFieldToFilter('comment', array('like' => 'Refund affiliate for Campaign'));
            
            foreach ($migrate_debits as $debit) {
                $this->_addToDb("
                            INSERT INTO `gomage_wallet_debit_tmp` VALUES(" . $debit->getId() . ", " . $wallet->getId() . ", 3, '" . $debit->getData('wallet_event_id') . "', '" . $debit->getData('event_sum') . "', '" . $debit->getData('comment') . "', " . $debit->getData('status') . ", '" . $debit->getData('created_at') . "', '" . $debit->getData('updated_at') . "');
                        ");
            }
            
            unset($migrate_debits);
            
            $migrate_debits = Mage::getModel('gomage_wallet/debit')
                ->getCollection();
            
            $migrate_debits->addFieldToFilter('wallet_id', $wallet->getId());
            $migrate_debits->addFieldToFilter('comment', array('like' => 'Affiliate debit for Campaign'));
            
            foreach ($migrate_debits as $debit) {
                $this->_addToDb("
                            INSERT INTO `gomage_wallet_debit_tmp` VALUES(" . $debit->getId() . ", " . $wallet->getId() . ", 3, '" . $debit->getData('wallet_event_id') . "', '" . $debit->getData('event_sum') . "', '" . $debit->getData('comment') . "', " . $debit->getData('status') . ", '" . $debit->getData('created_at') . "', '" . $debit->getData('updated_at') . "');
                        ");
            }
            
            unset($migrate_debits);
            
            $migrate_debits = Mage::getModel('gomage_wallet/debit')
                ->getCollection();
            
            $migrate_debits->addFieldToFilter('wallet_id', $wallet->getId());
            $migrate_debits->addFieldToFilter('wallet_event', array('eq' => 5));
            
            foreach ($migrate_debits as $debit) {
                $this->_addToDb("
                            INSERT INTO `gomage_wallet_debit_tmp` VALUES(" . $debit->getId() . ", " . $wallet->getId() . ", 5, '" . $debit->getData('wallet_event_id') . "', '" . $debit->getData('event_sum') . "', '" . $debit->getData('comment') . "', " . $debit->getData('status') . ", '" . $debit->getData('created_at') . "', '" . $debit->getData('updated_at') . "');
                        ");
            }
            
            unset($migrate_debits);
            
            $migrate_debits = Mage::getModel('gomage_customdashboard/refund')
                ->getCollection()
                ->addFieldToFilter('customer_id', $customerId)
                ->addFieldToFilter('transaction_status', 2);
            
            foreach ($migrate_debits as $debit) {
                $this->_addToDb("
                            INSERT INTO `gomage_wallet_debit_tmp` VALUES($di, " . $wallet->getId() . ", 2, '" . time() . "', '" . $debit->getData('total') . "', 'Dashboard refund request', 2, '" . $debit->getData('created_at') . "', '" . $debit->getData('updated_at') . "');
                        ");
                $di++;
            }
            
            unset($migrate_debits);
            
            // Send to db queries
            $this->_sendQueueToDb();
            
            $debits = $write->fetchOne("
                        SELECT SUM(`event_sum`) as `sum` FROM `gomage_wallet_debit_tmp` WHERE `wallet_id` = " . $wallet->getId() . " AND `status` = 2;
                    ");
            
            $debit = round($debits, 2);
            
            $credits = $write->fetchOne("
                        SELECT SUM(`event_sum`) as `sum` FROM `gomage_wallet_credit_tmp` WHERE `wallet_id` = " . $wallet->getId() . ";
                    ");
            
            $credit          = round($credits, 2);
            $migrate_balance = ($credit - $debit);
            $balance_diff    = round($wallet->getData('balance') - $migrate_balance, 2);
            
            if ($balance_diff > 0) {
                $write->query("
                            INSERT INTO `gomage_wallet_credit_tmp` VALUES($ci, " . $wallet->getId() . ", 1, '" . time() . "', '" . $balance_diff . "', 'Migration credit', 2, '" . $wallet->getData('created_at') . "', '" . $wallet->getData('created_at') . "');
                        ");
                $ci++;
            } else {
                if ($balance_diff < 0) {
                    $balance_diff *= -1;

                    $write->query("
                                INSERT INTO `gomage_wallet_debit_tmp` VALUES($di, " . $wallet->getId() . ", 2, '" . time() . "', '" . $balance_diff . "', 'Migration debit', 2, '" . $wallet->getData('created_at') . "', '" . $wallet->getData('created_at') . "');
                            ");
                    $di++;
                }
            }

            echo 'customer ' . $customerId . ' done (' . $wi . ' of ' . $wallets_count . ' within ' . (microtime(1) - $c_start) . ' s)' . PHP_EOL;
        }
        
        echo 'done within ' . (microtime(1) - $start) . ' s' . PHP_EOL;
    }

    /**
     * Migrate Wallet Credit
     */
    protected function _migrateCredit()
    {
        echo 'credit migration start' . PHP_EOL;

        $this->_connection->query("
                    DELETE FROM `gomage_wallet_credit`;
                ");

        $this->_connection->truncateTable('gomage_wallet_credit');

        $this->_connection->query("
            INSERT INTO `gomage_wallet_credit` (
                wallet_id,
                wallet_event,
                wallet_event_id,
                event_sum,
                comment,
                status,
                created_at,
                updated_at
            )
            SELECT 
                wallet_id,
                wallet_event,
                wallet_event_id,
                event_sum,
                comment,
                status,
                created_at,
                updated_at
            FROM gomage_wallet_credit_tmp ORDER BY created_at;
        ");

        echo 'credit migration end' . PHP_EOL;
    }
    
    /**
     * Migration data
     */
    protected function _migrate()
    {

        $this->_migrateCredit();

        $write = $this->_connection;

        $write->query("
                    CREATE TABLE IF NOT EXISTS `gomage_wallet_increments_tmp` (
                        `id` INT(11) NOT NULL AUTO_INCREMENT,
                        `old_id` INT(11) NOT NULL,
                        PRIMARY KEY (`id`)
                    );
                ");

        $write->truncateTable('gomage_wallet_increments_tmp');
        
        echo 'debit migration start' . PHP_EOL;

        $write->query("
            ALTER TABLE `gomage_wallet_debit_info`
	          DROP FOREIGN KEY `FK_gomage_wallet_debit_info_gomage_wallet_debit`;
        ");

        $write->query("
            DELETE FROM `gomage_wallet_debit`;
        ");

        $write->truncateTable('gomage_wallet_debit');

        $write->query("
            INSERT INTO `gomage_wallet_debit` (
                wallet_id,
                wallet_event,
                wallet_event_id,
                event_sum,
                comment,
                status,
                created_at,
                updated_at
            )
            SELECT 
                wallet_id,
                wallet_event,
                wallet_event_id,
                event_sum,
                comment,
                status,
                created_at,
                updated_at
            FROM gomage_wallet_debit_tmp ORDER BY created_at;
        ");

        echo 'debit migration end' . PHP_EOL;

        $write->query("
            INSERT INTO `gomage_wallet_increments_tmp` (`old_id`) 
            SELECT `id` FROM `gomage_wallet_debit_tmp` ORDER BY `created_at`;
        ");

        echo 'debit info update start' . PHP_EOL;
        
        $write->query("
            UPDATE gomage_wallet_debit_info AS di
            INNER JOIN gomage_wallet_increments_tmp AS i ON i.old_id = di.debit_id 
            SET di.debit_id=i.id;
        ");

        echo 'debit info update end' . PHP_EOL;

        $write->query("
            ALTER TABLE `gomage_wallet_debit_info`
                ADD CONSTRAINT `FK_gomage_wallet_debit_info_gomage_wallet_debit` 
                FOREIGN KEY (`debit_id`) 
                REFERENCES `gomage_wallet_debit` (`id`) 
                ON UPDATE CASCADE 
                ON DELETE CASCADE;
         ");

        $write->dropTable('gomage_wallet_increments_tmp');
    }
    
    /**
     * Run script
     */
    public function run()
    {
        try {
            if ($this->getArg('collect')) {
                $this->_collect();
            }
            
            if ($this->getArg('migrate')) {
                $this->_migrate();
            }
            
            if ($this->getArg('clean')) {
                $this->_clean();
            }
        } catch (Exception $e) {
            echo 'Exception. Look into ' . $this->getLogPath() . PHP_EOL;
            Mage::log($e->__toString(), Zend_Log::ERR, $this->getLogPath());
        }
    }
    
    /**
     * Clean tables
     */
    protected function _clean()
    {
        $dropTableList = [
            'gomage_wallet_credit_tmp',
            'gomage_wallet_debit_tmp',
            'gomage_wallet_increments_tmp',
        ];
        
        foreach ($dropTableList as $table) {
            try {
                $this->_connection->dropTable($table);
            } catch (Exception $e) {
                echo sprintf('Error in drop table %s', $table), PHP_EOL;
                echo $e->getMessage(), PHP_EOL;
            }
        }
    }
}

$shell = new GoMage_Shell_WalletMigrationSS();

$shell->run();
