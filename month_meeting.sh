python3 train.py --exp_name setup_n+0 --w2 1 --job_dir ../AUO_data/wip_data
python3 train.py --exp_name dps_n+0 --w3 1 --job_dir ../AUO_data/wip_data
python3 train.py --exp_name setup\&dps_n+0 --w2 0.5 --w3 0.5 --job_dir ../AUO_data/wip_data

python3 train.py --exp_name setup_n+1 --w2 1 --job_dir ../AUO_data/wip_data_n+1_subroutine
python3 train.py --exp_name dps_n+1 --w3 1 --job_dir ../AUO_data/wip_data_n+1_subroutine
python3 train.py --exp_name setup\&dps_n+1 --w2 0.5 --w3 0.5 --job_dir ../AUO_data/wip_data_n+1_subroutine
