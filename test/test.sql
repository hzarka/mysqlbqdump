
/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;

-- Dumping structure for table test.test_a
CREATE TABLE IF NOT EXISTS `test_a` (
`cint` int(11) DEFAULT NULL,
`cvarchar` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Dumping data for table test.test_a: ~2 rows (approximately)
DELETE FROM `test_a`;
/*!40000 ALTER TABLE `test_a` DISABLE KEYS */;
INSERT INTO `test_a` (`cint`, `cvarchar`) VALUES
(1, 'hello'),
(2, 'hi');
/*!40000 ALTER TABLE `test_a` ENABLE KEYS */;


-- Dumping structure for table test.test_b
CREATE TABLE IF NOT EXISTS `test_b` (
`cint` int(11) DEFAULT NULL,
`cvarchar` varchar(50) DEFAULT NULL,
`cdate` date DEFAULT NULL,
`ctimestamp` timestamp NULL DEFAULT NULL,
`cdouble` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Dumping data for table test.test_b: ~1 rows (approximately)
DELETE FROM `test_b`;
/*!40000 ALTER TABLE `test_b` DISABLE KEYS */;
INSERT INTO `test_b` (`cint`, `cvarchar`, `cdate`, `ctimestamp`, `cdouble`) VALUES
(1, 'hello', '2018-09-08', '2018-09-08 15:35:02', 12345.6789);
/*!40000 ALTER TABLE `test_b` ENABLE KEYS */;


-- Dumping structure for table test.test_c
CREATE TABLE IF NOT EXISTS `test_c` (
`cint` int(11) DEFAULT NULL,
`cvarchar` varchar(50) DEFAULT NULL,
`cdecimal` decimal(10,5) DEFAULT NULL,
`cbinary` binary(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Dumping data for table test.test_c: ~1 rows (approximately)
DELETE FROM `test_c`;
/*!40000 ALTER TABLE `test_c` DISABLE KEYS */;
INSERT INTO `test_c` (`cint`, `cvarchar`, `cdecimal`, `cbinary`) VALUES
(1, 'hello', 9876.54321, _binary 0x2137219372198371290000000000000000000000000000000000000000000000000000000000000000000000000000000000);
/*!40000 ALTER TABLE `test_c` ENABLE KEYS */;
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;

