import Test.Hspec
import qualified Data.PowerQueue.Worker.DistributedSpec

main :: IO ()
main =
    hspec $
    Data.PowerQueue.Worker.DistributedSpec.spec
